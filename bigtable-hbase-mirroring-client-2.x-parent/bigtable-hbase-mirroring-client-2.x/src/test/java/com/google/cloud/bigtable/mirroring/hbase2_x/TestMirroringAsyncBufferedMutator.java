/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.mirroring.hbase2_x;

import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createGet;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createGets;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createPut;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createResult;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerMock;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerToRejectRequests;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase2_x.utils.futures.FutureConverter;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestMirroringAsyncBufferedMutator {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock AsyncBufferedMutator primaryMutator;
  @Mock AsyncBufferedMutator secondaryMutator;
  @Mock FlowController flowController;

  MirroringAsyncBufferedMutator mirroringMutator;

  @Before
  public void setUp() {
    setupFlowControllerMock(flowController);
    this.mirroringMutator =
        spy(new MirroringAsyncBufferedMutator(primaryMutator, secondaryMutator, flowController));
  }

  @Test
  public void testResultIsCompletedOnPrimaryCompletion()
      throws ExecutionException, InterruptedException {
    Put put = new Put(Bytes.toBytes("rowKey"));
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("value"));

    CompletableFuture<Void> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Void> secondaryCalled = new CompletableFuture<>();
    when(primaryMutator.mutate(put)).thenReturn(primaryFuture);
    when(secondaryMutator.mutate(put))
        .thenAnswer(
            invocationOnMock -> {
              secondaryCalled.complete(null);
              return new CompletableFuture<>();
            });

    CompletableFuture<FlowController.ResourceReservation> resourcesAllocated =
        new CompletableFuture<>();
    when(flowController.asyncRequestResource(any(RequestResourcesDescription.class)))
        .thenReturn(FutureConverter.toListenable(resourcesAllocated));

    CompletableFuture<Void> resultFuture = mirroringMutator.mutate(put);

    // waiting for primary
    verify(primaryMutator, times(1)).mutate(put);
    verify(flowController, times(0)).asyncRequestResource(any(RequestResourcesDescription.class));
    assertThat(resultFuture.isDone()).isFalse();

    // primary complete but still waiting for resources so not done
    primaryFuture.complete(null);
    assertThat(resultFuture.isDone()).isFalse();

    // got resources so we got the result
    resourcesAllocated.complete(() -> {});
    resultFuture.get();

    // if we got the resources then the secondary should be scheduled
    secondaryCalled.get();
    verify(flowController, times(1)).asyncRequestResource(any(RequestResourcesDescription.class));
    verify(secondaryMutator, times(1)).mutate(put);
  }


  @Test
  public void testPrimaryFailed(){
    Put put = new Put(Bytes.toBytes("rowKey"));
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("value"));

    CompletableFuture<Void> primaryFailure = new CompletableFuture<>();
    CompletableFuture<Void> secondaryCalled = new CompletableFuture<>();

    when(primaryMutator.mutate(put)).thenReturn(primaryFailure);

    CompletableFuture<Void> resultFuture = mirroringMutator.mutate(put);
    primaryFailure.completeExceptionally(new RuntimeException());

    verify(primaryMutator, times(1)).mutate(put);
    verify(flowController, times(0)).asyncRequestResource(any(RequestResourcesDescription.class));
    verify(secondaryMutator, times(0)).mutate(put);
    assertThat(resultFuture.isCompletedExceptionally()).isTrue();
    assertThat(secondaryCalled.isDone()).isFalse();
  }

  @Test
  public void testRequestResourceFailed(){
    Put put = new Put(Bytes.toBytes("rowKey"));
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("value"));

    CompletableFuture<Void> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Void> secondaryCalled = new CompletableFuture<>();
    when(primaryMutator.mutate(put)).thenReturn(primaryFuture);

    CompletableFuture<FlowController.ResourceReservation> resourcesAllocated =
            new CompletableFuture<>();
    when(flowController.asyncRequestResource(any(RequestResourcesDescription.class)))
            .thenReturn(FutureConverter.toListenable(resourcesAllocated));


    CompletableFuture<Void> resultFuture = mirroringMutator.mutate(put);

    // waiting for primary
    verify(primaryMutator, times(1)).mutate(put);
    verify(flowController, times(0)).asyncRequestResource(any(RequestResourcesDescription.class));
    assertThat(resultFuture.isDone()).isFalse();

    // primary complete but still waiting for resources so not done
    primaryFuture.complete(null);

    assertThat(secondaryCalled.isDone()).isFalse();
    resourcesAllocated.completeExceptionally(new RuntimeException());
    try {
      resultFuture.get();
    } catch (InterruptedException | ExecutionException ignored) {}
    assertThat(resultFuture.isCompletedExceptionally()).isFalse();

    verify(flowController, times(1)).asyncRequestResource(any(RequestResourcesDescription.class));
  }

  @Test
  public void testSecondaryFailed()
          throws ExecutionException, InterruptedException {
    Put put = new Put(Bytes.toBytes("rowKey"));
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("value"));

    CompletableFuture<Void> primaryFuture = new CompletableFuture<>();
    CompletableFuture<Void> secondaryCalled = new CompletableFuture<>();
    CompletableFuture<Void> secondaryFailure = new CompletableFuture<>();
    when(primaryMutator.mutate(put)).thenReturn(primaryFuture);
    when(secondaryMutator.mutate(put))
            .thenAnswer(
                    invocationOnMock -> {
                      secondaryCalled.complete(null);
                      return secondaryFailure;
                    });

    CompletableFuture<FlowController.ResourceReservation> resourcesAllocated =
            new CompletableFuture<>();
    when(flowController.asyncRequestResource(any(RequestResourcesDescription.class)))
            .thenReturn(FutureConverter.toListenable(resourcesAllocated));

    CompletableFuture<Void> resultFuture = mirroringMutator.mutate(put);

    // waiting for primary
    verify(primaryMutator, times(1)).mutate(put);
    verify(flowController, times(0)).asyncRequestResource(any(RequestResourcesDescription.class));
    assertThat(resultFuture.isDone()).isFalse();

    // primary complete but still waiting for resources so not done
    primaryFuture.complete(null);
    assertThat(resultFuture.isDone()).isFalse();

    secondaryFailure.completeExceptionally(new RuntimeException());

    // got resources so we got the result
    resourcesAllocated.complete(() -> {});
    resultFuture.get();

    secondaryCalled.get();

    verify(flowController, times(1)).asyncRequestResource(any(RequestResourcesDescription.class));
    verify(secondaryMutator, times(1)).mutate(put);

    assertThat(resultFuture.isCompletedExceptionally()).isFalse();
  }

}

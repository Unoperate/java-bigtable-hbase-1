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
package com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator;

import static com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator.MirroringBufferedMutatorCommon.blockedFlushes;
import static com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator.MirroringBufferedMutatorCommon.flushWithErrors;
import static com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator.MirroringBufferedMutatorCommon.makeConfigurationWithFlushThreshold;
import static com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator.MirroringBufferedMutatorCommon.mutateWithErrors;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.hbase1_x.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestConcurrentMirroringBufferedMutator {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExecutorServiceRule executorServiceRule = ExecutorServiceRule.cachedPoolExecutor();

  public final MirroringBufferedMutatorCommon common = new MirroringBufferedMutatorCommon();

  @Test
  public void testBufferedWritesWithoutErrors() throws IOException, InterruptedException {
    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(common.mutation1);
    verify(common.primaryBufferedMutator, times(1)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(1)).mutate(ArgumentMatchers.<Mutation>anyList());
    bm.mutate(common.mutation1);
    verify(common.primaryBufferedMutator, times(2)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(2)).mutate(ArgumentMatchers.<Mutation>anyList());
    bm.mutate(common.mutation1);
    verify(common.primaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    bm.mutate(common.mutation1);
    Thread.sleep(300);
    executorServiceRule.waitForExecutor();
    verify(common.primaryBufferedMutator, times(4)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(4)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(1)).flush();
    verify(common.resourceReservation, times(4)).release();
  }

  @Test
  public void testBufferedMutatorFlush() throws IOException {
    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.flush();
    executorServiceRule.waitForExecutor();
    verify(common.primaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.primaryBufferedMutator, times(1)).flush();
    verify(common.secondaryBufferedMutator, times(1)).flush();
    verify(common.resourceReservation, times(3)).release();
  }

  @Test
  public void testCloseFlushesWrites() throws IOException {
    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.close();
    verify(common.primaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(3)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.primaryBufferedMutator, times(1)).flush();
    verify(common.secondaryBufferedMutator, times(1)).flush();
    verify(common.resourceReservation, times(3)).release();
  }

  @Test
  public void testCloseIsIdempotent() throws IOException {
    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);
    bm.close();
    bm.close();
    verify(common.secondaryBufferedMutator, times(1)).flush();
    verify(common.resourceReservation, times(3)).release();
  }

  @Test
  public void testFlushesCanBeScheduledSimultaneously()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    final AtomicInteger ongoingFlushes = new AtomicInteger(0);
    final SettableFuture<Void> allFlushesStarted = SettableFuture.create();
    final SettableFuture<Void> endFlush = SettableFuture.create();

    doAnswer(blockedFlushes(ongoingFlushes, allFlushesStarted, endFlush, 4))
        .when(common.primaryBufferedMutator)
        .flush();

    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 1.5));

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);

    bm.mutate(common.mutation1);
    bm.mutate(common.mutation1);

    allFlushesStarted.get(3, TimeUnit.SECONDS);
    assertThat(ongoingFlushes.get()).isEqualTo(4);
    endFlush.set(null);
    executorServiceRule.waitForExecutor();
    verify(common.primaryBufferedMutator, times(8)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(8)).mutate(ArgumentMatchers.<Mutation>anyList());
    verify(common.secondaryBufferedMutator, times(4)).flush();
    verify(common.resourceReservation, times(8)).release();
  }

  @Test
  public void testErrorsReportedByPrimaryDoNotPreventSecondaryWrites() throws IOException {
    doAnswer(
            mutateWithErrors(
                this.common.primaryBufferedMutatorParamsCaptor,
                common.primaryBufferedMutator,
                common.mutation1,
                common.mutation3))
        .when(common.primaryBufferedMutator)
        .mutate(ArgumentMatchers.<Mutation>anyList());

    BufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    try {
      bm.mutate(common.mutation1);
    } catch (IOException e) {
    }
    try {
      bm.mutate(common.mutation2);
    } catch (IOException e) {
    }
    try {
      bm.mutate(common.mutation3);
    } catch (IOException e) {
    }
    try {
      bm.mutate(common.mutation4);
    } catch (IOException e) {
    }
    executorServiceRule.waitForExecutor();
    verify(common.primaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation1));
    verify(common.secondaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation1));
    verify(common.primaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation2));
    verify(common.secondaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation2));
    verify(common.primaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation3));
    verify(common.secondaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation3));
    verify(common.primaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation4));
    verify(common.secondaryBufferedMutator, times(1)).mutate(Arrays.asList(common.mutation4));
  }

  @Test
  public void testErrorsReportedBySecondaryAreReportedAsWriteErrors() throws IOException {
    doAnswer(
            mutateWithErrors(
                this.common.secondaryBufferedMutatorParamsCaptor,
                common.secondaryBufferedMutator,
                common.mutation1,
                common.mutation3))
        .when(common.secondaryBufferedMutator)
        .mutate(ArgumentMatchers.<Mutation>anyList());

    MirroringBufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 3.5));

    bm.mutate(
        Arrays.asList(common.mutation1, common.mutation2, common.mutation3, common.mutation4));
    executorServiceRule.waitForExecutor();
    verify(common.secondaryBufferedMutator, times(1))
        .mutate(
            Arrays.asList(common.mutation1, common.mutation2, common.mutation3, common.mutation4));

    verify(common.secondaryWriteErrorConsumerWithMetrics, times(1))
        .consume(
            eq(HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST),
            eq(common.mutation1),
            any(Throwable.class));
    verify(common.secondaryWriteErrorConsumerWithMetrics, times(1))
        .consume(
            eq(HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST),
            eq(common.mutation3),
            any(Throwable.class));
  }

  @Test
  public void testErrorsInBothPrimaryAndSecondary() throws IOException {
    //    | primary | secondary |
    // m1 | v       | v         |
    // m2 | x       | v         |
    // m3 | v       | x         |
    // m4 | x       | x         |

    MirroringBufferedMutator bm = getBufferedMutator((long) (common.mutationSize * 1.5));

    doAnswer(
            flushWithErrors(
                this.common.primaryBufferedMutatorParamsCaptor,
                common.primaryBufferedMutator,
                common.mutation1,
                common.mutation3))
        .when(common.primaryBufferedMutator)
        .flush();
    doAnswer(
            flushWithErrors(
                this.common.secondaryBufferedMutatorParamsCaptor,
                common.secondaryBufferedMutator,
                common.mutation3,
                common.mutation4))
        .when(common.secondaryBufferedMutator)
        .flush();

    List<Mutation> mutations =
        Arrays.asList(common.mutation1, common.mutation2, common.mutation3, common.mutation4);
    bm.mutate(mutations);

    executorServiceRule.waitForExecutor();
    verify(common.secondaryBufferedMutator, times(1)).mutate(mutations);
    verify(common.secondaryBufferedMutator, times(1)).mutate(mutations);

    verify(common.primaryBufferedMutator, times(1)).flush();
    verify(common.secondaryBufferedMutator, times(1)).flush();

    verify(common.secondaryWriteErrorConsumerWithMetrics, times(1))
        .consume(
            eq(HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST),
            eq(common.mutation3),
            any(Throwable.class));
    // TODO: verify primary errors
  }

  private MirroringBufferedMutator getBufferedMutator(long flushThreshold) throws IOException {
    return new ConcurrentMirroringBufferedMutator(
        common.primaryConnection,
        common.secondaryConnection,
        common.bufferedMutatorParams,
        makeConfigurationWithFlushThreshold(flushThreshold),
        common.flowController,
        executorServiceRule.executorService,
        common.secondaryWriteErrorConsumerWithMetrics,
        new MirroringTracer());
  }
}

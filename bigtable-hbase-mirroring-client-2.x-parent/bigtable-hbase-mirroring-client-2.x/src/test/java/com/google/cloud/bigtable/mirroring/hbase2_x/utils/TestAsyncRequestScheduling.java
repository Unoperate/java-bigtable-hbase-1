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
package com.google.cloud.bigtable.mirroring.hbase2_x.utils;

import static com.google.cloud.bigtable.mirroring.hbase2_x.utils.AsyncRequestScheduling.reserveFlowControlResourcesThenScheduleSecondary;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.common.util.concurrent.FutureCallback;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.Test;

public class TestAsyncRequestScheduling {
  @Test
  public void testErrorHandlingInScheduler() throws ExecutionException, InterruptedException {
    CompletableFuture<Void> exceptionalFuture = new CompletableFuture<>();
    IOException ioe = new IOException("expected");
    exceptionalFuture.completeExceptionally(ioe);

    FlowController.ResourceReservation resourceReservation =
        spy(
            new FlowController.ResourceReservation() {
              @Override
              public void release() {}
            });
    CompletableFuture<FlowController.ResourceReservation> resourceReservationFuture =
        CompletableFuture.completedFuture(resourceReservation);

    Supplier<CompletableFuture<Void>> secondaryFutureSupplier =
        spy(
            new Supplier<CompletableFuture<Void>>() {
              @Override
              public CompletableFuture<Void> get() {
                return null;
              }
            });
    Function<Void, FutureCallback<Void>> verificationCreator =
        spy(
            new Function<Void, FutureCallback<Void>>() {
              @Override
              public FutureCallback<Void> apply(Void unused) {
                return null;
              }
            });

    CompletableFuture<Void> resultFuture =
        reserveFlowControlResourcesThenScheduleSecondary(
            resourceReservationFuture,
            exceptionalFuture,
            secondaryFutureSupplier,
            verificationCreator);

    final List<Throwable> resultFutureThrowableList = new ArrayList<>();
    resultFuture
        .exceptionally(
            t -> {
              resultFutureThrowableList.add(t);
              return null;
            })
        .get();

    assertThat(resultFutureThrowableList.size()).isEqualTo(1);
    assertThat(resultFutureThrowableList.get(0)).isEqualTo(ioe);

    verify(resourceReservation, times(1)).release();
    verify(verificationCreator, never()).apply((Void) any());
    verify(secondaryFutureSupplier, never()).get();

    assertThat(resourceReservationFuture.isCancelled());
  }
}

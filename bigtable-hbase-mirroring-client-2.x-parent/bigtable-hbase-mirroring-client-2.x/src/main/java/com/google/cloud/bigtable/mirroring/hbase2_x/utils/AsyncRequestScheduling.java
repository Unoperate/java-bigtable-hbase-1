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

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase2_x.utils.futures.FutureUtils;
import com.google.common.util.concurrent.FutureCallback;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Supplier;

public class AsyncRequestScheduling {
  public static <T>
      ResultWithVerificationCompletion<CompletableFuture<T>>
          reserveFlowControlResourcesThenScheduleSecondary(
              final CompletableFuture<T> primaryFuture,
              final CompletableFuture<FlowController.ResourceReservation> reservationFuture,
              final Supplier<CompletableFuture<T>> secondaryFutureSupplier,
              final Function<T, FutureCallback<T>> verificationCreator) {
    return reserveFlowControlResourcesThenScheduleSecondary(
        primaryFuture, reservationFuture, secondaryFutureSupplier, verificationCreator, () -> {});
  }

  public static <T>
      ResultWithVerificationCompletion<CompletableFuture<T>>
          reserveFlowControlResourcesThenScheduleSecondary(
              final CompletableFuture<T> primaryFuture,
              final CompletableFuture<FlowController.ResourceReservation> reservationFuture,
              final Supplier<CompletableFuture<T>> secondaryFutureSupplier,
              final Function<T, FutureCallback<T>> verificationCreator,
              final Runnable flowControlReservationErrorHandler) {
    ResultWithVerificationCompletion<CompletableFuture<T>> returnedValue =
        new ResultWithVerificationCompletion<>(new CompletableFuture<>());

    primaryFuture
        .thenAccept(
            (primaryResult) ->
                reservationFuture
                    .whenComplete(
                        (ignoredResult, ignoredError) ->
                            returnedValue.result.complete(primaryResult))
                    .thenAccept(
                        reservation ->
                            FutureUtils.forwardResult(
                                scheduleVerificationAfterSecondaryOperation(
                                    reservation,
                                    secondaryFutureSupplier.get(),
                                    verificationCreator.apply(primaryResult)),
                                returnedValue.getVerificationCompletedFuture()))
                    .exceptionally(
                        reservationError -> {
                          flowControlReservationErrorHandler.run();
                          returnedValue.verificationCompleted();
                          throw new CompletionException(reservationError);
                        }))
        .exceptionally(
            primaryError -> {
              FlowController.cancelRequest(reservationFuture);
              returnedValue.result.completeExceptionally(
                  FutureUtils.unwrapCompletionException(primaryError));
              returnedValue.verificationCompleted();
              throw new CompletionException(primaryError);
            });

    return returnedValue;
  }

  private static <T> CompletableFuture<Void> scheduleVerificationAfterSecondaryOperation(
      final FlowController.ResourceReservation reservation,
      final CompletableFuture<T> secondaryFuture,
      final FutureCallback<T> verificationCallback) {
    return secondaryFuture.handle(
        (secondaryResult, secondaryError) -> {
          try {
            if (secondaryError != null) {
              verificationCallback.onFailure(secondaryError);
            } else {
              verificationCallback.onSuccess(secondaryResult);
            }
          } finally {
            reservation.release();
          }
          return null;
        });
  }

  public static class ResultWithVerificationCompletion<T> {
    public final T result;
    private final CompletableFuture<Void> verificationEndedFuture;

    public ResultWithVerificationCompletion(T result) {
      this.result = result;
      this.verificationEndedFuture = new CompletableFuture<>();
    }

    public void verificationCompleted() {
      this.verificationEndedFuture.complete(null);
    }

    public CompletableFuture<Void> getVerificationCompletedFuture() {
      return this.verificationEndedFuture;
    }
  }
}

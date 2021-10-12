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

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.AccumulatedExceptions;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController.ResourceReservation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

public class ConcurrentMirroringBufferedMutator extends MirroringBufferedMutator {
  private final Map<Row, Throwable> failedPrimaryOperations = new ConcurrentHashMap<>();
  private final Map<Row, Throwable> failedSecondaryOperations = new ConcurrentHashMap<>();

  private final LinkedList<Throwable> primaryAsyncFlushExceptions = new LinkedList<>();

  public ConcurrentMirroringBufferedMutator(
      Connection primaryConnection,
      Connection secondaryConnection,
      BufferedMutatorParams bufferedMutatorParams,
      MirroringConfiguration configuration,
      FlowController flowController,
      ExecutorService executorService,
      SecondaryWriteErrorConsumer secondaryWriteErrorConsumer,
      MirroringTracer mirroringTracer)
      throws IOException {
    super(
        primaryConnection,
        secondaryConnection,
        bufferedMutatorParams,
        configuration,
        flowController,
        executorService,
        secondaryWriteErrorConsumer,
        mirroringTracer);
  }

  @Override
  protected void mutateScoped(final List<? extends Mutation> mutations) throws IOException {
    RequestResourcesDescription resourcesDescription = new RequestResourcesDescription(mutations);
    ListenableFuture<ResourceReservation> reservationFuture =
        flowController.asyncRequestResource(resourcesDescription);

    ResourceReservation reservation;
    try {
      try (Scope scope = this.mirroringTracer.spanFactory.flowControlScope()) {
        reservation = reservationFuture.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      // We won't write those mutations to secondary database, they should be reported to
      // secondaryWriteErrorConsumer.
      reportWriteErrors(mutations, e);
      setInterruptedFlagInInterruptedException(e);
      throw new IOException(e);
    }

    AccumulatedExceptions exceptions = new AccumulatedExceptions();

    // Primary write
    try {
      this.mirroringTracer.spanFactory.wrapPrimaryOperation(
          new CallableThrowingIOException<Void>() {
            @Override
            public Void call() throws IOException {
              primaryBufferedMutator.mutate(mutations);
              return null;
            }
          },
          HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST);
    } catch (RetriesExhaustedWithDetailsException e) {
      // Exceptions thrown by primary operations should be rethrown to the user.
      saveExceptionToBeThrown(e);
    } catch (IOException e) {
      exceptions.add(e);
    }

    // Secondary write
    try {
      this.mirroringTracer.spanFactory.wrapSecondaryOperation(
          new CallableThrowingIOException<Void>() {
            @Override
            public Void call() throws IOException {
              secondaryBufferedMutator.mutate(mutations);
              return null;
            }
          },
          HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST);
    } catch (RetriesExhaustedWithDetailsException e) {
      // Ignore this error, it was already handled by error handler.
    } catch (IOException e) {
      exceptions.add(e);
    }

    storeResourcesAndFlushIfNeeded(mutations, resourcesDescription, reservation);

    exceptions.rethrowIfCaptured();

    // Throw exceptions from async flush.
    throwPrimaryFlushExceptionIfAvailable();
    // Throw exceptions that were thrown by ExceptionListener on primary BufferedMutator which we
    // have caught when calling flush.
    throwExceptionIfAvailable();
  }

  @Override
  protected void scopedFlush() throws IOException {
    try {
      scheduleFlush().secondaryFlushFinished.get();
    } catch (InterruptedException | ExecutionException e) {
      setInterruptedFlagInInterruptedException(e);
      throw new IOException(e);
    }
    throwPrimaryFlushExceptionIfAvailable();
    throwExceptionIfAvailable();
  }

  @Override
  protected void handlePrimaryException(RetriesExhaustedWithDetailsException e) {
    for (int i = 0; i < e.getNumExceptions(); i++) {
      failedPrimaryOperations.put(e.getRow(i), e.getCause(i));
    }
  }

  @Override
  protected void handleSecondaryException(RetriesExhaustedWithDetailsException e) {
    for (int i = 0; i < e.getNumExceptions(); i++) {
      failedSecondaryOperations.put(e.getRow(i), e.getCause(i));
    }
  }

  private static class MutationWithErrorCause {
    public final Mutation mutation;
    public final Throwable cause;

    private MutationWithErrorCause(Mutation mutation, Throwable cause) {
      this.mutation = mutation;
      this.cause = cause;
    }
  }

  @Override
  protected synchronized FlushFutures scheduleFlushScoped(
      final List<? extends Mutation> dataToFlush,
      final List<ResourceReservation> flushReservations) {
    final SettableFuture<Void> bothFlushesFinished = SettableFuture.create();

    ListenableFuture<Void> primaryFlushFinished = schedulePrimaryFlush();
    ListenableFuture<Void> secondaryFlushFinished = scheduleSecondaryFlush();

    final AtomicBoolean firstFinished = new AtomicBoolean(false);
    final Runnable flushFinished =
        new Runnable() {
          @Override
          public void run() {
            if (firstFinished.getAndSet(true)) {
              bothFlushesFinishedCallback(dataToFlush, flushReservations);
              bothFlushesFinished.set(null);
            }
          }
        };

    Futures.addCallback(
        primaryFlushFinished,
        this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
            new FutureCallback<Void>() {
              @Override
              public void onSuccess(@NullableDecl Void aVoid) {
                flushFinished.run();
              }

              @Override
              public void onFailure(Throwable throwable) {
                // All writes that failed on secondary should already be stored in
                // `failedPrimaryOperations`.
                if (throwable instanceof RetriesExhaustedWithDetailsException) {
                  // But we should rethrow RetriesExhaustedWithDetailsExceptions to the user.
                  saveExceptionToBeThrown((RetriesExhaustedWithDetailsException) throwable);
                } else {
                  // `flush` can only throw IOExceptions
                  assert throwable instanceof IOException || throwable instanceof RuntimeException;
                  primaryAsyncFlushExceptions.addLast(throwable);
                }
                flushFinished.run();
              }
            }),
        MoreExecutors.directExecutor());

    Futures.addCallback(
        secondaryFlushFinished,
        this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
            new FutureCallback<Void>() {
              @Override
              public void onSuccess(@NullableDecl Void aVoid) {
                flushFinished.run();
              }

              @Override
              public void onFailure(Throwable throwable) {
                // All writes that failed on secondary should already be stored in
                // `failedSecondaryOperations`.
                flushFinished.run();
              }
            }),
        MoreExecutors.directExecutor());

    return new FlushFutures(primaryFlushFinished, bothFlushesFinished);
  }

  private ListenableFuture<Void> scheduleSecondaryFlush() {
    return this.executorService.submit(
        this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
            new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                mirroringTracer.spanFactory.wrapSecondaryOperation(
                    new CallableThrowingIOException<Void>() {
                      @Override
                      public Void call() throws IOException {
                        secondaryBufferedMutator.flush();
                        return null;
                      }
                    },
                    HBaseOperation.BUFFERED_MUTATOR_FLUSH);
                return null;
              }
            }));
  }

  private void bothFlushesFinishedCallback(
      List<? extends Mutation> dataToFlush, List<ResourceReservation> flushReservations) {
    List<MutationWithErrorCause> secondaryErrors = new ArrayList<>();
    for (Mutation mutation : dataToFlush) {
      Throwable primaryCause = failedPrimaryOperations.remove(mutation);
      Throwable secondaryCause = failedSecondaryOperations.remove(mutation);
      boolean primaryFailed = primaryCause != null;
      boolean secondaryFailed = secondaryCause != null;

      // Primary errors are ignored - appropriate callbacks have been called and the errors will be
      // reported to the user in the usual way.
      if (secondaryFailed && !primaryFailed) {
        secondaryErrors.add(new MutationWithErrorCause(mutation, secondaryCause));
      }
    }

    if (!secondaryErrors.isEmpty()) {
      try (Scope scope = mirroringTracer.spanFactory.writeErrorScope()) {
        for (MutationWithErrorCause mutationAndCause : secondaryErrors) {
          secondaryWriteErrorConsumer.consume(
              HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST,
              mutationAndCause.mutation,
              mutationAndCause.cause);
        }
      }
    }

    releaseReservations(flushReservations);
  }

  private void throwPrimaryFlushExceptionIfAvailable() throws IOException {
    if (this.primaryAsyncFlushExceptions.isEmpty()) {
      return;
    }
    Throwable error = this.primaryAsyncFlushExceptions.pollFirst();
    if (error instanceof IOException) {
      throw (IOException) error;
    } else if (error instanceof RuntimeException) {
      throw (RuntimeException) error;
    } else {
      throw new RuntimeException(error);
    }
  }
}

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

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration;
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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

@InternalApi("For internal usage only")
public class SequentialMirroringBufferedMutator extends MirroringBufferedMutator {
  /**
   * Set of {@link Row}s that were passed to primary BufferedMutator but failed. We create a entry
   * in this collection every time our error handler is called by primary BufferedMutator. Those
   * entries are consulted before we perform mutations on secondary BufferedMutator, if a {@link
   * Row} instance scheduled for insertion is in this collection, then it is omitted and
   * corresponding entry is removed from the set.
   */
  private final Set<Row> failedPrimaryOperations =
      Collections.newSetFromMap(new ConcurrentHashMap<Row, Boolean>());

  public SequentialMirroringBufferedMutator(
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
  protected void mutateScoped(final List<? extends Mutation> list) throws IOException {
    IOException primaryException = null;
    try {
      this.mirroringTracer.spanFactory.wrapPrimaryOperation(
          new CallableThrowingIOException<Void>() {
            @Override
            public Void call() throws IOException {
              primaryBufferedMutator.mutate(list);
              return null;
            }
          },
          HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST);
    } catch (IOException e) {
      primaryException = e;
    } finally {
      // This call might block - we have confirmed that mutate() calls on BufferedMutator from
      // HBase client library might also block.
      addSecondaryMutation(list, primaryException);
    }
    // Throw exceptions that were thrown by ExceptionListener on primary BufferedMutator which we
    // have caught when calling flush.
    throwExceptionIfAvailable();
  }

  /**
   * This method is called from within {@code finally} block. Currently processed exception is
   * passed in primaryException, if any. This method shouldn't throw any exception if
   * primaryException != null.
   */
  private void addSecondaryMutation(
      List<? extends Mutation> mutations, IOException primaryException) throws IOException {
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
      if (primaryException != null) {
        // We are currently in a finally block handling an exception, we shouldn't throw anything.
        primaryException.addSuppressed(e);
        return;
      } else {
        throw new IOException(e);
      }
    }

    storeResourcesAndFlushIfNeeded(mutations, resourcesDescription, reservation);
  }

  @Override
  protected void scopedFlush() throws IOException {
    try {
      scheduleFlush().primaryFlushFinished.get();
    } catch (InterruptedException | ExecutionException e) {
      setInterruptedFlagInInterruptedException(e);
      throw new IOException(e);
    }
    throwExceptionIfAvailable();
  }

  @Override
  protected void handlePrimaryException(RetriesExhaustedWithDetailsException e) {
    for (int i = 0; i < e.getNumExceptions(); i++) {
      failedPrimaryOperations.add(e.getRow(i));
    }
  }

  @Override
  protected void handleSecondaryException(RetriesExhaustedWithDetailsException e) {
    reportWriteErrors(e);
  }

  @Override
  protected synchronized FlushFutures scheduleFlushScoped(
      final List<? extends Mutation> dataToFlush,
      final List<ResourceReservation> flushReservations) {
    final SettableFuture<Void> secondaryFlushFinished = SettableFuture.create();

    ListenableFuture<Void> primaryFlushFinished = schedulePrimaryFlush();

    Futures.addCallback(
        primaryFlushFinished,
        this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
            new FutureCallback<Void>() {
              @Override
              public void onSuccess(@NullableDecl Void aVoid) {
                performSecondaryFlush(dataToFlush, flushReservations, secondaryFlushFinished);
              }

              @Override
              public void onFailure(Throwable throwable) {
                if (throwable instanceof RetriesExhaustedWithDetailsException) {
                  // If user-defined listener has thrown an exception
                  // (RetriesExhaustedWithDetailsException is the only exception that can be
                  // thrown), we know that some of the writes failed. Our handler has already
                  // handled those errors. We should also rethrow this exception when user
                  // calls mutate/flush the next time.
                  saveExceptionToBeThrown((RetriesExhaustedWithDetailsException) throwable);

                  performSecondaryFlush(dataToFlush, flushReservations, secondaryFlushFinished);
                } else {
                  // In other cases, we do not know what caused the error and we have no idea
                  // what was really written to the primary DB, the best we can do is write
                  // them to on-disk log. Trying to save them to secondary database is not a
                  // good idea - if current thread was interrupted then next flush might also
                  // be, only increasing our confusion, moreover, that may cause secondary
                  // writes that were not completed on primary.
                  reportWriteErrors(dataToFlush, throwable);
                  releaseReservations(flushReservations);
                  secondaryFlushFinished.setException(throwable);
                }
              }
            }),
        MoreExecutors.directExecutor());
    return new FlushFutures(primaryFlushFinished, secondaryFlushFinished);
  }

  private void performSecondaryFlush(
      List<? extends Mutation> dataToFlush,
      List<ResourceReservation> flushReservations,
      SettableFuture<Void> completionFuture) {
    final List<? extends Mutation> successfulOperations = removeFailedMutations(dataToFlush);
    try {
      if (!successfulOperations.isEmpty()) {
        this.mirroringTracer.spanFactory.wrapSecondaryOperation(
            new CallableThrowingIOException<Void>() {
              @Override
              public Void call() throws IOException {
                secondaryBufferedMutator.mutate(successfulOperations);
                return null;
              }
            },
            HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST);

        this.mirroringTracer.spanFactory.wrapSecondaryOperation(
            new CallableThrowingIOException<Void>() {
              @Override
              public Void call() throws IOException {
                secondaryBufferedMutator.flush();
                return null;
              }
            },
            HBaseOperation.BUFFERED_MUTATOR_FLUSH);
      }
      releaseReservations(flushReservations);
      completionFuture.set(null);
    } catch (Throwable e) {
      // Our listener is registered and should catch non-fatal errors. This is either
      // InterruptedIOException or some RuntimeError, in both cases we should consider operation as
      // not completed - the worst that can happen is that we will have some writes in both
      // secondary database and on-disk log.
      reportWriteErrors(dataToFlush, e);
      releaseReservations(flushReservations);
      completionFuture.setException(e);
    }
  }

  private List<? extends Mutation> removeFailedMutations(List<? extends Mutation> dataToFlush) {
    List<Mutation> successfulMutations = new ArrayList<>();
    for (Mutation mutation : dataToFlush) {
      if (!this.failedPrimaryOperations.remove(mutation)) {
        successfulMutations.add(mutation);
      }
    }
    return successfulMutations;
  }
}

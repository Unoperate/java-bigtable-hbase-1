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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;

/**
 * BufferedMutator that mirrors writes performed on first database to secondary database.
 *
 * <p>We want to perform a secondary write only if we are certain that it was successfully applied
 * on primary database. The HBase 1.x API doesn't give its user any indication when asynchronous
 * writes were performed, only performing a synchronous {@link BufferedMutator#flush()} ensures that
 * all previously buffered mutations are done. To achieve our goal we store a copy of all mutations
 * sent to primary BufferedMutator in a internal buffer. When size of the buffer reaches a threshold
 * of {@link
 * com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper#MIRRORING_BUFFERED_MUTATOR_BYTES_TO_FLUSH}
 * bytes, we perform a flush in a worker thread. After flush we pass collected mutations to
 * secondary BufferedMutator and flush it. Writes that have failed on primary are not forwarded to
 * secondary, writes that have failed on secondary are forwarded to {@link
 * SecondaryWriteErrorConsumer#consume(HBaseOperation, Mutation, Throwable)} handler.
 *
 * <p>Moreover, we perform our custom flow control to prevent unbounded growth of memory - calls to
 * mutate() might block if secondary database lags behind. We account size of all operations that
 * were placed in primary BufferedMutator but weren't yet executed and confirmed on secondary
 * BufferedMutator (or until we are informed that they have failed on primary).
 */
@InternalApi("For internal usage only")
public abstract class MirroringBufferedMutator implements BufferedMutator {
  public static MirroringBufferedMutator create(
      boolean concurrent,
      Connection primaryConnection,
      Connection secondaryConnection,
      BufferedMutatorParams bufferedMutatorParams,
      MirroringConfiguration configuration,
      FlowController flowController,
      ExecutorService executorService,
      SecondaryWriteErrorConsumer secondaryWriteErrorConsumer,
      MirroringTracer mirroringTracer)
      throws IOException {
    if (concurrent) {
      return new ConcurrentMirroringBufferedMutator(
          primaryConnection,
          secondaryConnection,
          bufferedMutatorParams,
          configuration,
          flowController,
          executorService,
          secondaryWriteErrorConsumer,
          mirroringTracer);
    } else {
      return new SequentialMirroringBufferedMutator(
          primaryConnection,
          secondaryConnection,
          bufferedMutatorParams,
          configuration,
          flowController,
          executorService,
          secondaryWriteErrorConsumer,
          mirroringTracer);
    }
  }

  protected final BufferedMutator primaryBufferedMutator;
  protected final BufferedMutator secondaryBufferedMutator;
  protected final FlowController flowController;
  protected final ListeningExecutorService executorService;
  protected final SecondaryWriteErrorConsumer secondaryWriteErrorConsumer;
  protected final MirroringTracer mirroringTracer;

  /** Configuration that was used to configure this instance. */
  private final Configuration configuration;
  /** Parameters that were used to create this instance. */
  private final BufferedMutatorParams bufferedMutatorParams;
  /** Size that {@link #mutationsBuffer} should reach to invoke a flush() on primary database. */
  protected final long mutationsBufferFlushIntervalBytes;

  /**
   * Internal buffer with mutations that were passed to primary BufferedMutator but were not yet
   * scheduled to be confirmed and written to the secondary database in {@link #scheduleFlush()}.
   */
  protected ArrayList<Mutation> mutationsBuffer;

  protected long mutationsBufferSizeBytes;

  /**
   * {@link ResourceReservation}s obtained from {@link #flowController} that represent resources
   * used by mutations kept in {@link #mutationsBuffer}.
   */
  protected ArrayList<ResourceReservation> reservations;

  /**
   * Exceptions caught when performing asynchronous flush() on primary BufferedMutator that should
   * be rethrown to inform the user about failed writes.
   */
  private List<RetriesExhaustedWithDetailsException> exceptionsToBeThrown = new ArrayList<>();

  private boolean closed = false;

  public MirroringBufferedMutator(
      Connection primaryConnection,
      Connection secondaryConnection,
      BufferedMutatorParams bufferedMutatorParams,
      MirroringConfiguration configuration,
      FlowController flowController,
      ExecutorService executorService,
      SecondaryWriteErrorConsumer secondaryWriteErrorConsumer,
      MirroringTracer mirroringTracer)
      throws IOException {
    final ExceptionListener userListener = bufferedMutatorParams.getListener();
    ExceptionListener primaryErrorsListener =
        new ExceptionListener() {
          @Override
          public void onException(
              RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator)
              throws RetriesExhaustedWithDetailsException {
            handlePrimaryException(e);
            userListener.onException(e, bufferedMutator);
          }
        };

    ExceptionListener secondaryErrorsListener =
        new ExceptionListener() {
          @Override
          public void onException(
              RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator)
              throws RetriesExhaustedWithDetailsException {
            handleSecondaryException(e);
          }
        };

    this.primaryBufferedMutator =
        primaryConnection.getBufferedMutator(
            copyMutatorParamsAndSetListener(bufferedMutatorParams, primaryErrorsListener));
    this.secondaryBufferedMutator =
        secondaryConnection.getBufferedMutator(
            copyMutatorParamsAndSetListener(bufferedMutatorParams, secondaryErrorsListener));
    this.flowController = flowController;
    this.mutationsBufferFlushIntervalBytes =
        configuration.mirroringOptions.bufferedMutatorBytesToFlush;
    this.executorService = MoreExecutors.listeningDecorator(executorService);
    this.configuration = configuration;
    this.bufferedMutatorParams = bufferedMutatorParams;
    this.secondaryWriteErrorConsumer = secondaryWriteErrorConsumer;

    this.mutationsBuffer = new ArrayList<>();
    this.reservations = new ArrayList<>();
    this.mirroringTracer = mirroringTracer;
  }

  @Override
  public void mutate(Mutation mutation) throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.BUFFERED_MUTATOR_MUTATE)) {
      mutateScoped(Collections.singletonList(mutation));
    }
  }

  @Override
  public void mutate(final List<? extends Mutation> list) throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(
            HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST)) {
      mutateScoped(list);
    }
  }

  protected abstract void mutateScoped(final List<? extends Mutation> list) throws IOException;

  @Override
  public void flush() throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.BUFFERED_MUTATOR_FLUSH)) {
      scopedFlush();
    }
  }

  protected abstract void scopedFlush() throws IOException;

  abstract void handlePrimaryException(RetriesExhaustedWithDetailsException e);

  abstract void handleSecondaryException(RetriesExhaustedWithDetailsException e);

  protected final synchronized void storeResourcesAndFlushIfNeeded(
      List<? extends Mutation> mutations,
      RequestResourcesDescription resourcesDescription,
      ResourceReservation reservation)
      throws IOException {
    this.mutationsBuffer.addAll(mutations);
    this.reservations.add(reservation);
    this.mutationsBufferSizeBytes += resourcesDescription.sizeInBytes;
    if (this.mutationsBufferSizeBytes > this.mutationsBufferFlushIntervalBytes) {
      // We are not afraid of multiple simultaneous flushes:
      // - HBase clients are thread-safe.
      // - Each failed Row should be reported and placed in `failedPrimaryOperations` once.
      // - Each issued Row will be consulted with `failedPrimaryOperations` only once, because
      //   each flush sets up a clean buffer for incoming mutations.
      scheduleFlush();
    }
  }

  protected final void reportWriteErrors(RetriesExhaustedWithDetailsException e) {
    try (Scope scope = this.mirroringTracer.spanFactory.writeErrorScope()) {
      for (int i = 0; i < e.getNumExceptions(); i++) {
        this.secondaryWriteErrorConsumer.consume(
            HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST, e.getRow(i), e.getCause(i));
      }
    }
  }

  protected final void reportWriteErrors(List<? extends Mutation> mutations, Throwable cause) {
    try (Scope scope = this.mirroringTracer.spanFactory.writeErrorScope()) {
      this.secondaryWriteErrorConsumer.consume(
          HBaseOperation.BUFFERED_MUTATOR_MUTATE_LIST, mutations, cause);
    }
  }

  @Override
  public final synchronized void close() throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.BUFFERED_MUTATOR_CLOSE)) {
      if (this.closed) {
        this.mirroringTracer
            .spanFactory
            .getCurrentSpan()
            .addAnnotation("MirroringBufferedMutator closed more than once.");
        return;
      }
      this.closed = true;

      List<IOException> exceptions = new ArrayList<>();

      try {
        scheduleFlush().secondaryFlushFinished.get();
      } catch (InterruptedException | ExecutionException e) {
        setInterruptedFlagInInterruptedException(e);
        exceptions.add(new IOException(e));
      }
      try {
        this.mirroringTracer.spanFactory.wrapPrimaryOperation(
            new CallableThrowingIOException<Void>() {
              @Override
              public Void call() throws IOException {
                MirroringBufferedMutator.this.primaryBufferedMutator.close();
                return null;
              }
            },
            HBaseOperation.BUFFERED_MUTATOR_CLOSE);
      } catch (IOException e) {
        exceptions.add(e);
      }
      try {
        this.mirroringTracer.spanFactory.wrapSecondaryOperation(
            new CallableThrowingIOException<Void>() {
              @Override
              public Void call() throws IOException {
                MirroringBufferedMutator.this.secondaryBufferedMutator.close();
                return null;
              }
            },
            HBaseOperation.BUFFERED_MUTATOR_CLOSE);
      } catch (IOException e) {
        exceptions.add(e);
      }
      if (!exceptions.isEmpty()) {
        Iterator<IOException> exceptionIterator = exceptions.iterator();
        IOException firstException = exceptionIterator.next();
        while (exceptionIterator.hasNext()) {
          firstException.addSuppressed(exceptionIterator.next());
        }
        throw firstException;
      }
    }
  }

  @Override
  public long getWriteBufferSize() {
    return this.bufferedMutatorParams.getWriteBufferSize();
  }

  @Override
  public TableName getName() {
    return this.bufferedMutatorParams.getTableName();
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  private BufferedMutatorParams copyMutatorParamsAndSetListener(
      BufferedMutatorParams bufferedMutatorParams, ExceptionListener exceptionListener) {
    BufferedMutatorParams params = new BufferedMutatorParams(bufferedMutatorParams.getTableName());
    params.writeBufferSize(bufferedMutatorParams.getWriteBufferSize());
    params.pool(bufferedMutatorParams.getPool());
    params.maxKeyValueSize(bufferedMutatorParams.getMaxKeyValueSize());
    params.listener(exceptionListener);
    return params;
  }

  protected static class FlushFutures {
    ListenableFuture<Void> primaryFlushFinished;
    ListenableFuture<Void> secondaryFlushFinished;

    public FlushFutures(
        ListenableFuture<Void> primaryFlushFinished, SettableFuture<Void> secondaryFlushFinished) {
      this.primaryFlushFinished = primaryFlushFinished;
      this.secondaryFlushFinished = secondaryFlushFinished;
    }
  }

  protected final synchronized FlushFutures scheduleFlush() {
    try (Scope scope = this.mirroringTracer.spanFactory.scheduleFlushScope()) {
      this.mutationsBufferSizeBytes = 0;

      final List<? extends Mutation> dataToFlush = this.mutationsBuffer;
      this.mutationsBuffer = new ArrayList<>();

      final List<ResourceReservation> flushReservations = this.reservations;
      this.reservations = new ArrayList<>();
      return scheduleFlushScoped(dataToFlush, flushReservations);
    }
  }

  protected abstract FlushFutures scheduleFlushScoped(
      List<? extends Mutation> dataToFlush, List<ResourceReservation> flushReservations);

  protected final ListenableFuture<Void> schedulePrimaryFlush() {
    return this.executorService.submit(
        this.mirroringTracer.spanFactory.wrapWithCurrentSpan(
            new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                mirroringTracer.spanFactory.wrapPrimaryOperation(
                    new CallableThrowingIOException<Void>() {
                      @Override
                      public Void call() throws IOException {
                        primaryBufferedMutator.flush();
                        return null;
                      }
                    },
                    HBaseOperation.BUFFERED_MUTATOR_FLUSH);
                return null;
              }
            }));
  }

  protected final synchronized void saveExceptionToBeThrown(
      RetriesExhaustedWithDetailsException exception) {
    this.exceptionsToBeThrown.add(exception);
  }

  protected final RetriesExhaustedWithDetailsException getExceptionsToBeThrown() {
    List<RetriesExhaustedWithDetailsException> exceptions;
    synchronized (this) {
      if (this.exceptionsToBeThrown.isEmpty()) {
        return null;
      }
      exceptions = this.exceptionsToBeThrown;
      this.exceptionsToBeThrown = new ArrayList<>();
    }

    List<Row> rows = new ArrayList<>();
    List<Throwable> causes = new ArrayList<>();
    List<String> hostnames = new ArrayList<>();

    for (RetriesExhaustedWithDetailsException e : exceptions) {
      for (int i = 0; i < e.getNumExceptions(); i++) {
        rows.add(e.getRow(i));
        causes.add(e.getCause(i));
        hostnames.add(e.getHostnamePort(i));
      }
    }
    return new RetriesExhaustedWithDetailsException(causes, rows, hostnames);
  }

  protected final void throwExceptionIfAvailable() throws RetriesExhaustedWithDetailsException {
    RetriesExhaustedWithDetailsException e = getExceptionsToBeThrown();
    if (e != null) {
      throw e;
    }
  }

  protected final void releaseReservations(List<ResourceReservation> flushReservations) {
    for (ResourceReservation reservation : flushReservations) {
      reservation.release();
    }
  }

  protected final void setInterruptedFlagInInterruptedException(Exception e) {
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }
}

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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers.AsyncTableWrapper;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableCloseable;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Logger;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringSpan;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.RequestScheduling;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.RunnableThrowingIOException;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Service;
import org.apache.hadoop.hbase.util.Bytes;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

/**
 * Table which mirrors every two mutations to two underlying tables.
 *
 * <p>Objects of this class present themselves as HBase 1.x `Table` objects. Every operation is
 * first performed on primary table and if it succeeded it is replayed on the secondary table
 * asynchronously. Read operations are mirrored to verify that content of both databases matches.
 */
@InternalApi("For internal usage only")
public class MirroringTable implements Table, ListenableCloseable {
  private static final Logger Log = new Logger(MirroringTable.class);

  Table primaryTable;
  Table secondaryTable;
  AsyncTableWrapper secondaryAsyncWrapper;
  VerificationContinuationFactory verificationContinuationFactory;
  private FlowController flowController;
  private ListenableReferenceCounter referenceCounter;
  private boolean closed = false;

  /**
   * @param executorService ExecutorService is used to perform operations on secondaryTable and
   *     verification tasks.
   * @param mismatchDetector Detects mismatches in results from operations preformed on both
   *     databases.
   */
  public MirroringTable(
      Table primaryTable,
      Table secondaryTable,
      ExecutorService executorService,
      MismatchDetector mismatchDetector,
      FlowController flowController) {
    this.primaryTable = primaryTable;
    this.secondaryTable = secondaryTable;
    this.verificationContinuationFactory = new VerificationContinuationFactory(mismatchDetector);
    this.secondaryAsyncWrapper =
        new AsyncTableWrapper(
            this.secondaryTable, MoreExecutors.listeningDecorator(executorService));
    this.flowController = flowController;
    this.referenceCounter = new ListenableReferenceCounter();
    this.referenceCounter.holdReferenceUntilClosing(this.secondaryAsyncWrapper);
  }

  @Override
  public TableName getName() {
    return this.primaryTable.getName();
  }

  @Override
  public Configuration getConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exists(final Get get) throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.exists(Get)");
    Log.trace("[%s] exists(get=%s)", this.getName(), get);

    boolean result =
        span.wrapPrimaryOperation(
            new CallableThrowingIOException<Boolean>() {
              @Override
              public Boolean call() throws IOException {
                return MirroringTable.this.primaryTable.exists(get);
              }
            });

    scheduleVerificationAndRequestWithFlowControl(
        new RequestResourcesDescription(result),
        this.secondaryAsyncWrapper.exists(get, span),
        this.verificationContinuationFactory.exists(get, result),
        span);
    return result;
  }

  @Override
  public boolean[] existsAll(final List<Get> list) throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.existsAll(List<Get>)");
    Log.trace("[%s] existsAll(gets=%s)", this.getName(), list);

    boolean[] result =
        span.wrapPrimaryOperation(
            new CallableThrowingIOException<boolean[]>() {
              @Override
              public boolean[] call() throws IOException {
                return MirroringTable.this.primaryTable.existsAll(list);
              }
            });

    scheduleVerificationAndRequestWithFlowControl(
        new RequestResourcesDescription(result),
        this.secondaryAsyncWrapper.existsAll(list, span),
        this.verificationContinuationFactory.existsAll(list, result),
        span);
    return result;
  }

  private void batchWithSpan(
      final List<? extends Row> operations, final Object[] results, MirroringSpan span)
      throws IOException, InterruptedException {
    Log.trace("[%s] batch(operations=%s, results)", this.getName(), operations);
    try {
      this.primaryTable.batch(operations, results);
    } finally {
      // An exception indicates operation that failed at least partially.
      BatchHelpers.scheduleSecondaryWriteBatchOperations(operations, results, this, span);
    }
  }

  @Override
  public void batch(List<? extends Row> operations, Object[] results)
      throws IOException, InterruptedException {
    MirroringSpan span = new MirroringSpan("MirroringTable.batch(List<Row>, Object[]))");
    batchWithSpan(operations, results, span);
  }

  @Override
  public Object[] batch(List<? extends Row> operations) throws IOException, InterruptedException {
    Log.trace("[%s] batch(operations=%s)", this.getName(), operations);
    Object[] results = new Object[operations.size()];
    this.batch(operations, results);
    return results;
  }

  @Override
  public <R> void batchCallback(
      List<? extends Row> operations, Object[] results, Callback<R> callback)
      throws IOException, InterruptedException {
    MirroringSpan span = new MirroringSpan("MirroringTable.batchCallback()");
    Log.trace(
        "[%s] batchCallback(operations=%s, results, callback=%s)",
        this.getName(), operations, callback);
    try {
      this.primaryTable.batchCallback(operations, results, callback);
    } finally {
      BatchHelpers.scheduleSecondaryWriteBatchOperations(operations, results, this, span);
    }
  }

  @Override
  public <R> Object[] batchCallback(List<? extends Row> operations, Callback<R> callback)
      throws IOException, InterruptedException {
    Log.trace(
        "[%s] batchCallback(operations=%s, callback=%s)", this.getName(), operations, callback);
    Object[] results = new Object[operations.size()];
    this.batchCallback(operations, results, callback);
    return results;
  }

  @Override
  public Result get(final Get get) throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.get(Get)");
    Log.trace("[%s] get(get=%s)", this.getName(), get);

    Result result =
        span.wrapPrimaryOperation(
            new CallableThrowingIOException<Result>() {
              @Override
              public Result call() throws IOException {
                return MirroringTable.this.primaryTable.get(get);
              }
            });

    scheduleVerificationAndRequestWithFlowControl(
        new RequestResourcesDescription(result),
        this.secondaryAsyncWrapper.get(get, span),
        this.verificationContinuationFactory.get(get, result),
        span);

    return result;
  }

  @Override
  public Result[] get(final List<Get> list) throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.get(List<Get>)");
    Log.trace("[%s] get(gets=%s)", this.getName(), list);

    Result[] result =
        span.wrapPrimaryOperation(
            new CallableThrowingIOException<Result[]>() {
              @Override
              public Result[] call() throws IOException {
                return MirroringTable.this.primaryTable.get(list);
              }
            });

    scheduleVerificationAndRequestWithFlowControl(
        new RequestResourcesDescription(result),
        this.secondaryAsyncWrapper.get(list, span),
        this.verificationContinuationFactory.get(list, result),
        span);

    return result;
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.getScanner(Scan)");
    try {
      Log.trace("[%s] getScanner(scan=%s)", this.getName(), scan);
      MirroringResultScanner scanner =
          new MirroringResultScanner(
              scan,
              this.primaryTable.getScanner(scan),
              this.secondaryAsyncWrapper,
              this.verificationContinuationFactory,
              this.flowController);
      this.referenceCounter.holdReferenceUntilClosing(scanner);
      span.end();
      return scanner;
    } catch (Exception e) {
      span.endWithError();
      throw e;
    }
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    return getScanner(new Scan().addFamily(family));
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
    return getScanner(new Scan().addColumn(family, qualifier));
  }

  /**
   * `close()` won't perform the actual close if there are any in-flight requests, in such a case
   * the `close` operation is scheduled and will be performed after all requests have finished.
   */
  @Override
  public void close() throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.close()");
    try {
      this.asyncClose();
      span.end();
    } catch (Exception e) {
      span.endWithError();
      throw e;
    }
  }

  public synchronized ListenableFuture<Void> asyncClose() throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.asyncClose()");
    Log.trace("[%s] asyncClose()", this.getName());
    if (closed) {
      return this.referenceCounter.getOnLastReferenceClosed();
    }

    this.closed = true;
    this.referenceCounter.decrementReferenceCount();

    IOException primaryException = null;
    try {
      span.wrapPrimaryOperation(
          new RunnableThrowingIOException() {
            @Override
            public void run() throws IOException {
              MirroringTable.this.primaryTable.close();
            }
          });
    } catch (IOException e) {
      primaryException = e;
    }

    try {
      try (Scope scope = span.secondaryOperationsScope()) {
        this.secondaryAsyncWrapper.asyncClose();
      }
    } catch (RuntimeException e) {
      span.endWithError();
      if (primaryException != null) {
        primaryException.addSuppressed(e);
        throw primaryException;
      } else {
        throw e;
      }
    }

    if (primaryException != null) {
      throw primaryException;
    }
    span.endWhenCompleted(this.referenceCounter.getOnLastReferenceClosed());
    return this.referenceCounter.getOnLastReferenceClosed();
  }

  @Override
  public void put(final Put put) throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.put(Put)");
    Log.trace("[%s] put(put=%s)", this.getName(), put);

    span.wrapPrimaryOperation(
        new RunnableThrowingIOException() {
          @Override
          public void run() throws IOException {
            MirroringTable.this.primaryTable.put(put);
          }
        });
    scheduleWriteWithControlFlow(
        new WriteOperationInfo(put),
        this.secondaryAsyncWrapper.put(put, span),
        this.flowController,
        span);
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.put(List<Put>)");
    Log.trace("[%s] put(puts=%s)", this.getName(), puts);
    try {
      Object[] results = new Object[puts.size()];
      this.batchWithSpan(puts, results, span);
    } catch (InterruptedException e) {
      IOException e2 = new InterruptedIOException();
      e2.initCause(e);
      throw e2;
    }
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
      throws IOException {
    Log.trace(
        "[%s] checkAndPut(row=%s, family=%s, qualifier=%s, value=%s, put=%s)",
        this.getName(), row, family, qualifier, value, put);
    return this.checkAndPut(row, family, qualifier, CompareOp.EQUAL, value, put);
  }

  @Override
  public boolean checkAndPut(
      byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value, Put put)
      throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.checkAndPut()");
    Log.trace(
        "[%s] checkAndPut(row=%s, family=%s, qualifier=%s, compareOp=%s, value=%s, put=%s)",
        this.getName(), row, family, qualifier, compareOp, value, put);
    RowMutations mutations = new RowMutations(row);
    mutations.add(put);
    return this.checkAndMutateWithSpan(row, family, qualifier, compareOp, value, mutations, span);
  }

  @Override
  public void delete(final Delete delete) throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.delete(Delete)");
    Log.trace("[%s] delete(delete=%s)", this.getName(), delete);
    span.wrapPrimaryOperation(
        new RunnableThrowingIOException() {
          @Override
          public void run() throws IOException {
            MirroringTable.this.primaryTable.delete(delete);
          }
        });
    scheduleWriteWithControlFlow(
        new WriteOperationInfo(delete),
        this.secondaryAsyncWrapper.delete(delete, span),
        this.flowController,
        span);
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.delete(List<Delete>)");
    Log.trace("[%s] delete(deletes=%s)", this.getName(), deletes);
    // Delete should remove successfully deleted rows from input list.
    Object[] results = new Object[deletes.size()];
    try {
      this.batchWithSpan(deletes, results, span);
      deletes.clear();
    } catch (InterruptedException e) {
      final BatchHelpers.SplitBatchResponse<Delete> splitResponse =
          new BatchHelpers.SplitBatchResponse<>(deletes, results);

      deletes.clear();
      deletes.addAll(splitResponse.failedWrites);

      IOException e2 = new InterruptedIOException();
      e2.initCause(e);
      throw e2;
    }
  }

  @Override
  public boolean checkAndDelete(
      byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
    Log.trace(
        "[%s] checkAndDelete(row=%s, family=%s, qualifier=%s, value=%s, delete=%s)",
        this.getName(), row, family, qualifier, value, delete);
    return this.checkAndDelete(row, family, qualifier, CompareOp.EQUAL, value, delete);
  }

  @Override
  public boolean checkAndDelete(
      byte[] row, byte[] family, byte[] qualifier, CompareOp compareOp, byte[] value, Delete delete)
      throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.checkAndDelete()");
    Log.trace(
        "[%s] checkAndDelete(row=%s, family=%s, qualifier=%s, compareOp=%s, value=%s, delete=%s)",
        this.getName(), row, family, qualifier, compareOp, value, delete);
    RowMutations mutations = new RowMutations(row);
    mutations.add(delete);
    return this.checkAndMutateWithSpan(row, family, qualifier, compareOp, value, mutations, span);
  }

  @Override
  public void mutateRow(final RowMutations rowMutations) throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.mutateRow(RowMutation)");
    Log.trace("[%s] mutateRow(rowMutations=%s)", this.getName(), rowMutations);

    span.wrapPrimaryOperation(
        new RunnableThrowingIOException() {
          @Override
          public void run() throws IOException {
            MirroringTable.this.primaryTable.mutateRow(rowMutations);
          }
        });

    scheduleWriteWithControlFlow(
        new WriteOperationInfo(rowMutations),
        this.secondaryAsyncWrapper.mutateRow(rowMutations, span),
        this.flowController,
        span);
  }

  @Override
  public Result append(final Append append) throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.append(Append)");
    Log.trace("[%s] append(append=%s)", this.getName(), append);

    Result result =
        span.wrapPrimaryOperation(
            new CallableThrowingIOException<Result>() {
              @Override
              public Result call() throws IOException {
                return MirroringTable.this.primaryTable.append(append);
              }
            });

    scheduleWriteWithControlFlow(
        new WriteOperationInfo(append),
        this.secondaryAsyncWrapper.append(append, span),
        this.flowController,
        span);
    return result;
  }

  @Override
  public Result increment(final Increment increment) throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.increment(Increment)");
    Log.trace("[%s] increment(increment=%s)", this.getName(), increment);

    Result result =
        span.wrapPrimaryOperation(
            new CallableThrowingIOException<Result>() {
              @Override
              public Result call() throws IOException {
                return MirroringTable.this.primaryTable.increment(increment);
              }
            });

    scheduleWriteWithControlFlow(
        new WriteOperationInfo(increment),
        this.secondaryAsyncWrapper.increment(increment, span),
        this.flowController,
        span);
    return result;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
      throws IOException {
    Log.trace(
        "[%s] incrementColumnValue(row=%s, family=%s, qualifier=%s, amount=%s)",
        this.getName(), row, family, qualifier, amount);
    Result result = increment((new Increment(row)).addColumn(family, qualifier, amount));
    Cell cell = result.getColumnLatestCell(family, qualifier);
    assert cell != null;
    return Bytes.toLong(CellUtil.cloneValue(cell));
  }

  @Override
  public long incrementColumnValue(
      byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability)
      throws IOException {
    Log.trace(
        "[%s] incrementColumnValue(row=%s, family=%s, qualifier=%s, amount=%s, durability=%s)",
        this.getName(), row, family, qualifier, amount, durability);
    Result result =
        increment(
            (new Increment(row)).addColumn(family, qualifier, amount).setDurability(durability));
    Cell cell = result.getColumnLatestCell(family, qualifier);
    assert cell != null;
    return Bytes.toLong(CellUtil.cloneValue(cell));
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(
      Class<T> aClass, byte[] bytes, byte[] bytes1, Call<T, R> call) throws Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Service, R> void coprocessorService(
      Class<T> aClass, byte[] bytes, byte[] bytes1, Call<T, R> call, Callback<R> callback)
      throws Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getWriteBufferSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWriteBufferSize(long l) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes1, R r)
      throws Throwable {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R extends Message> void batchCoprocessorService(
      MethodDescriptor methodDescriptor,
      Message message,
      byte[] bytes,
      byte[] bytes1,
      R r,
      Callback<R> callback)
      throws Throwable {
    throw new UnsupportedOperationException();
  }

  private boolean checkAndMutateWithSpan(
      final byte[] row,
      final byte[] family,
      final byte[] qualifier,
      final CompareOp compareOp,
      final byte[] value,
      final RowMutations rowMutations,
      MirroringSpan span)
      throws IOException {
    boolean wereMutationsApplied =
        span.wrapPrimaryOperation(
            new CallableThrowingIOException<Boolean>() {
              @Override
              public Boolean call() throws IOException {
                return MirroringTable.this.primaryTable.checkAndMutate(
                    row, family, qualifier, compareOp, value, rowMutations);
              }
            });

    if (wereMutationsApplied) {
      scheduleWriteWithControlFlow(
          new WriteOperationInfo(rowMutations),
          this.secondaryAsyncWrapper.mutateRow(rowMutations, span),
          this.flowController,
          span);
    }
    return wereMutationsApplied;
  }

  @Override
  public boolean checkAndMutate(
      byte[] row,
      byte[] family,
      byte[] qualifier,
      CompareOp compareOp,
      byte[] value,
      RowMutations rowMutations)
      throws IOException {
    MirroringSpan span = new MirroringSpan("MirroringTable.checkAndMutate()");
    Log.trace(
        "[%s] checkAndMutate(row=%s, family=%s, qualifier=%s, compareOp=%s, value=%s, rowMutations=%s)",
        this.getName(), row, family, qualifier, compareOp, value, rowMutations);

    return checkAndMutateWithSpan(row, family, qualifier, compareOp, value, rowMutations, span);
  }

  @Override
  public int getOperationTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setOperationTimeout(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getRpcTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRpcTimeout(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getReadRpcTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setReadRpcTimeout(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getWriteRpcTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setWriteRpcTimeout(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addOnCloseListener(Runnable listener) {
    this.referenceCounter
        .getOnLastReferenceClosed()
        .addListener(listener, MoreExecutors.directExecutor());
  }

  private <T> void scheduleVerificationAndRequestWithFlowControl(
      final RequestResourcesDescription resultInfo,
      final Supplier<ListenableFuture<T>> secondaryGetFutureSupplier,
      final FutureCallback<T> verificationCallback,
      MirroringSpan span) {
    this.referenceCounter.holdReferenceUntilCompletion(
        RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
            resultInfo,
            secondaryGetFutureSupplier,
            span.wrapVerificationCallback(verificationCallback),
            this.flowController,
            span));
  }

  public <T> void scheduleWriteWithControlFlow(
      final WriteOperationInfo writeOperationInfo,
      final Supplier<ListenableFuture<T>> secondaryResultFutureSupplier,
      final FlowController flowController,
      MirroringSpan span) {
    WriteOperationFutureCallback<T> writeErrorCallback =
        new WriteOperationFutureCallback<T>() {
          @Override
          public void onFailure(Throwable throwable) {
            handleFailedOperations(writeOperationInfo.operations);
          }
        };

    this.referenceCounter.holdReferenceUntilCompletion(
        RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
            writeOperationInfo.requestResourcesDescription,
            secondaryResultFutureSupplier,
            span.wrapWriteOperationCallback(writeErrorCallback),
            flowController,
            span));
  }

  public void handleFailedOperations(List<? extends Row> operations) {
    // TODO(mwalkiewicz): call write error handler.
  }

  public static class WriteOperationInfo {
    final RequestResourcesDescription requestResourcesDescription;
    final List<? extends Row> operations;

    public WriteOperationInfo(BatchHelpers.SplitBatchResponse<? extends Row> primarySplitResponse) {
      this.operations = primarySplitResponse.allSuccessfulOperations;
      this.requestResourcesDescription =
          new RequestResourcesDescription(
              this.operations, primarySplitResponse.successfulReadsResults);
    }

    public WriteOperationInfo(Put operation) {
      this(new RequestResourcesDescription(operation), operation);
    }

    public WriteOperationInfo(Delete operation) {
      this(new RequestResourcesDescription(operation), operation);
    }

    public WriteOperationInfo(Append operation) {
      this(new RequestResourcesDescription(operation), operation);
    }

    public WriteOperationInfo(Increment operation) {
      this(new RequestResourcesDescription(operation), operation);
    }

    public WriteOperationInfo(RowMutations operation) {
      this(new RequestResourcesDescription(operation), operation);
    }

    private WriteOperationInfo(
        RequestResourcesDescription requestResourcesDescription, Row operation) {
      this.requestResourcesDescription = requestResourcesDescription;
      this.operations = Collections.singletonList(operation);
    }
  }

  static class BatchHelpers {
    private static void scheduleSecondaryWriteBatchOperations(
        final List<? extends Row> operations,
        final Object[] results,
        MirroringTable table,
        MirroringSpan span) {

      final SplitBatchResponse<?> primarySplitResponse =
          new SplitBatchResponse<>(operations, results);

      if (primarySplitResponse.allSuccessfulOperations.size() == 0) {
        return;
      }

      final Object[] resultsSecondary =
          new Object[primarySplitResponse.allSuccessfulOperations.size()];

      FutureCallback<Void> verificationFuture =
          createBatchVerificationCallback(
              primarySplitResponse,
              resultsSecondary,
              table.verificationContinuationFactory.getMismatchDetector(),
              table,
              span);

      RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
          new WriteOperationInfo(primarySplitResponse).requestResourcesDescription,
          table.secondaryAsyncWrapper.batch(
              primarySplitResponse.allSuccessfulOperations, resultsSecondary, span),
          verificationFuture,
          table.flowController,
          span);
    }

    private static FutureCallback<Void> createBatchVerificationCallback(
        final SplitBatchResponse<?> primarySplitResponse,
        final Object[] secondaryResults,
        final MismatchDetector mismatchDetector,
        final MirroringTable table,
        final MirroringSpan span) {
      return new FutureCallback<Void>() {
        @Override
        public void onSuccess(@NullableDecl Void t) {
          // Batch is successful - all results are correct.
          List<? extends Row> secondaryOperations = primarySplitResponse.allSuccessfulOperations;

          final SplitBatchResponse<?> secondarySplitResponse =
              new SplitBatchResponse<>(secondaryOperations, secondaryResults);

          if (secondarySplitResponse.successfulReads.size() > 0) {
            try (Scope scope = span.verificationScope()) {
              mismatchDetector.batch(
                  secondarySplitResponse.successfulReads,
                  primarySplitResponse.successfulReadsResults,
                  secondarySplitResponse.successfulReadsResults);
            }
          }
        }

        @Override
        public void onFailure(Throwable throwable) {
          // Batch has thrown - partial results might be available.
          List<? extends Row> secondaryOperations = primarySplitResponse.allSuccessfulOperations;

          final SplitBatchResponse<?> secondarySplitResponse =
              new SplitBatchResponse<>(secondaryOperations, secondaryResults);

          if (secondarySplitResponse.failedWrites.size() > 0) {
            try (Scope scope = span.writeErrorScope()) {
              table.handleFailedOperations(secondarySplitResponse.failedWrites);
            }
          }

          if (secondarySplitResponse.allReads.size() > 0) {
            // Some of the reads in this batch might have been not successful.
            // We want to verify successful reads and report the others.

            // We are using `secondaryResults` to select indices of operations that were successful.
            // Using those indices we select Get operations that have results from both primary and
            // secondary database, and pass them to `mismatchDetector.batch()`.
            // We also gather failed gets to pass them to `batchGetFailure`.
            MatchingSuccessfulReadsResults matchingSuccessfulReads =
                selectMatchingSuccessfulReads(
                    secondaryOperations,
                    primarySplitResponse.allSuccessfulResults,
                    secondaryResults);

            try (Scope scope = span.verificationScope()) {
              mismatchDetector.batch(
                  secondarySplitResponse.successfulReads,
                  matchingSuccessfulReads.primaryResults,
                  matchingSuccessfulReads.secondaryResults);

              if (!matchingSuccessfulReads.failedReads.isEmpty()) {
                mismatchDetector.batch(matchingSuccessfulReads.failedReads, throwable);
              }
            }
          }
        }
      };
    }

    /**
     * Creates a {@link MatchingSuccessfulReadsResults} based on arrays of results from primary and
     * secondary databases and list of performed operations. All inputs are iterated simultaneously,
     * Get operations are identified using isinstance and f their results from both databases are
     * available, they are added to lists of matching reads and successful operations. In the other
     * case the Get operation is placed on failed operations list.
     */
    private static MatchingSuccessfulReadsResults selectMatchingSuccessfulReads(
        List<? extends Row> operations, Object[] primaryResults, Object[] secondaryResults) {
      assert operations.size() == secondaryResults.length;
      assert primaryResults.length == secondaryResults.length;

      List<Result> primaryMatchingReads = new ArrayList<>();
      List<Result> secondaryMatchingReads = new ArrayList<>();

      List<Get> failedReads = new ArrayList<>();
      List<Get> successfulReads = new ArrayList<>();

      for (int i = 0; i < secondaryResults.length; i++) {
        if (!(operations.get(i) instanceof Get)) {
          continue;
        }

        // We are sure casts are correct, and non-null results to Gets are always Results.
        if (secondaryResults[i] == null) {
          failedReads.add((Get) operations.get(i));
        } else {
          primaryMatchingReads.add((Result) primaryResults[i]);
          secondaryMatchingReads.add((Result) secondaryResults[i]);
          successfulReads.add((Get) operations.get(i));
        }
      }

      return new MatchingSuccessfulReadsResults(
          primaryMatchingReads.toArray(new Result[0]),
          secondaryMatchingReads.toArray(new Result[0]),
          failedReads,
          successfulReads);
    }

    /**
     * Helper class that facilitates analysing results of partially completed batch operation
     * containing {@link Get}s. Contains matching results from first and secondary databases, Get
     * operations that produced those results, and Gets that failed on secondary.
     */
    private static class MatchingSuccessfulReadsResults {
      final Result[] primaryResults;
      final Result[] secondaryResults;
      final List<Get> failedReads;
      final List<Get> successfulReads;

      private MatchingSuccessfulReadsResults(
          Result[] primaryResults,
          Result[] secondaryResults,
          List<Get> failedReads,
          List<Get> successfulReads) {
        this.primaryResults = primaryResults;
        this.secondaryResults = secondaryResults;
        this.failedReads = failedReads;
        this.successfulReads = successfulReads;
      }
    }

    /**
     * Helper class facilitating analysis of batch results. Basing on issued operations and results
     * array splits provided operations into reads/writes, failed/successful.
     */
    private static class SplitBatchResponse<T extends Row> {
      final List<Get> successfulReads = new ArrayList<>();
      final List<T> failedWrites = new ArrayList<>();
      final List<T> successfulWrites = new ArrayList<>();
      final List<T> allSuccessfulOperations = new ArrayList<>();
      final Result[] successfulReadsResults;
      final List<Get> allReads = new ArrayList<>();
      final Result[] allReadsResults;
      final Object[] allSuccessfulResults;

      SplitBatchResponse(List<T> operations, Object[] results) {
        final List<Result> successfulReadsResults = new ArrayList<>();
        final List<Result> allReadsResults = new ArrayList<>();
        final List<Object> allSuccessfulResultsList = new ArrayList<>();

        for (int i = 0; i < operations.size(); i++) {
          T operation = operations.get(i);
          boolean isRead = operation instanceof Get;
          boolean isFailed = results[i] == null;
          if (isFailed) {
            if (isRead) {
              this.allReads.add((Get) operation);
              allReadsResults.add(null);
            } else {
              this.failedWrites.add(operation);
            }
          } else {
            if (isRead) {
              this.successfulReads.add((Get) operation);
              successfulReadsResults.add((Result) results[i]);

              this.allReads.add((Get) operation);
              allReadsResults.add((Result) results[i]);
            } else {
              this.successfulWrites.add(operation);
            }
            this.allSuccessfulOperations.add(operation);
            allSuccessfulResultsList.add(results[i]);
          }
        }
        this.successfulReadsResults = successfulReadsResults.toArray(new Result[0]);
        this.allReadsResults = allReadsResults.toArray(new Result[0]);
        this.allSuccessfulResults = allSuccessfulResultsList.toArray(new Object[0]);
      }
    }
  }
}

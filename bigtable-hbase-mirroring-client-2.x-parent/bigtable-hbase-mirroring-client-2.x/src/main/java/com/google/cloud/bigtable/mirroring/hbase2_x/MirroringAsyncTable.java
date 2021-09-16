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

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringTable;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.RequestScheduling;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.cloud.bigtable.mirroring.hbase2_x.utils.futures.FutureConverter;
import com.google.common.util.concurrent.FutureCallback;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScanResultConsumerBase;
import org.apache.hadoop.hbase.client.ServiceCaller;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcChannel;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

public class MirroringAsyncTable<C extends ScanResultConsumerBase> implements AsyncTable<C> {
  private final AsyncTable<C> primaryTable;
  private final AsyncTable<C> secondaryTable;
  private final VerificationContinuationFactory verificationContinuationFactory;
  private final ExecutorService executorService;
  private final FlowController flowController;

  public MirroringAsyncTable(
      AsyncTable<C> primaryTable,
      AsyncTable<C> secondaryTable,
      ExecutorService executorService,
      MismatchDetector mismatchDetector,
      FlowController flowController) {
    this.primaryTable = primaryTable;
    this.secondaryTable = secondaryTable;
    this.executorService = executorService;
    this.verificationContinuationFactory = new VerificationContinuationFactory(mismatchDetector);
    this.flowController = flowController;
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    CompletableFuture<Result> future = new CompletableFuture<Result>();

    this.primaryTable
        .get(get)
        .handleAsync(
            (result, error) -> {
              if (error != null) {
                future.completeExceptionally(error);
              } else {
                scheduleVerificationAndRequestWithFlowControl(
                    new RequestResourcesDescription(result),
                    () -> {
                      future.complete(result);
                      return this.secondaryTable.get(get);
                    },
                    this.verificationContinuationFactory.get(get, result));
              }
              return null;
            },
            executorService);
    return future;
  }

  @Override
  public CompletableFuture<Boolean> exists(Get get) {
    CompletableFuture<Boolean> future = new CompletableFuture<Boolean>();

    this.primaryTable
        .exists(get)
        .handleAsync(
            (result, error) -> {
              if (error != null) {
                future.completeExceptionally(error);
              } else {
                scheduleVerificationAndRequestWithFlowControl(
                    new RequestResourcesDescription(result),
                    () -> {
                      future.complete(result);
                      return this.secondaryTable.exists(get);
                    },
                    this.verificationContinuationFactory.exists(get, result));
              }
              return null;
            },
            executorService);
    return future;
  }

  @Override
  public CompletableFuture<Void> put(Put put) {
    CompletableFuture<Void> future = new CompletableFuture<Void>();

    this.primaryTable
        .put(put)
        .handleAsync(
            (result, error) -> {
              if (error != null) {
                future.completeExceptionally(error);
              } else {
                scheduleWriteWithControlFlow(
                    new MirroringTable.WriteOperationInfo(put),
                    () -> {
                      future.complete(result);
                      return this.secondaryTable.put(put);
                    });
              }
              return null;
            },
            executorService);
    return future;
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    CompletableFuture<Void> future = new CompletableFuture<Void>();

    this.primaryTable
        .delete(delete)
        .handleAsync(
            (result, error) -> {
              if (error != null) {
                future.completeExceptionally(error);
              } else {
                scheduleWriteWithControlFlow(
                    new MirroringTable.WriteOperationInfo(delete),
                    () -> {
                      future.complete(result);
                      return this.secondaryTable.delete(delete);
                    });
              }
              return null;
            },
            executorService);
    return future;
  }

  @Override
  public CompletableFuture<Result> append(Append append) {
    CompletableFuture<Result> future = new CompletableFuture<Result>();

    this.primaryTable
        .append(append)
        .handleAsync(
            (result, error) -> {
              if (error != null) {
                future.completeExceptionally(error);
              } else {
                scheduleWriteWithControlFlow(
                    new MirroringTable.WriteOperationInfo(append),
                    () -> {
                      future.complete(result);
                      return this.secondaryTable.append(append);
                    });
              }
              return null;
            },
            executorService);
    return future;
  }

  @Override
  public CompletableFuture<Result> increment(Increment increment) {
    CompletableFuture<Result> future = new CompletableFuture<Result>();

    this.primaryTable
        .increment(increment)
        .handleAsync(
            (result, error) -> {
              if (error != null) {
                future.completeExceptionally(error);
              } else {
                scheduleWriteWithControlFlow(
                    new MirroringTable.WriteOperationInfo(increment),
                    () -> {
                      future.complete(result);
                      return this.secondaryTable.increment(increment);
                    });
              }
              return null;
            },
            executorService);
    return future;
  }

  @Override
  public CompletableFuture<Void> mutateRow(RowMutations rowMutations) {
    CompletableFuture<Void> future = new CompletableFuture<Void>();

    this.primaryTable
        .mutateRow(rowMutations)
        .handleAsync(
            (result, error) -> {
              if (error != null) {
                future.completeExceptionally(error);
              } else {
                scheduleWriteWithControlFlow(
                    new MirroringTable.WriteOperationInfo(rowMutations),
                    () -> {
                      future.complete(result);
                      return this.secondaryTable.mutateRow(rowMutations);
                    });
              }
              return null;
            },
            executorService);
    return future;
  }

  private <T> void scheduleVerificationAndRequestWithFlowControl(
      final RequestResourcesDescription resultInfo,
      final Supplier<CompletableFuture<T>> futureCompleterAndSecondaryResultFutureCaller,
      final FutureCallback<T> verificationCallback) {
    RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
        resultInfo,
        () -> FutureConverter.toListenable(futureCompleterAndSecondaryResultFutureCaller.get()),
        verificationCallback,
        this.flowController);
  }

  private <T> void scheduleWriteWithControlFlow(
      final MirroringTable.WriteOperationInfo writeOperationInfo,
      final Supplier<CompletableFuture<T>> secondaryResultFutureCaller) {
    RequestScheduling.scheduleVerificationAndRequestWithFlowControl(
        writeOperationInfo.requestResourcesDescription,
        () -> FutureConverter.toListenable(secondaryResultFutureCaller.get()),
        new FutureCallback<T>() {
          @Override
          public void onSuccess(@NullableDecl T t) {}

          @Override
          public void onFailure(Throwable throwable) {
            handleFailedOperations(writeOperationInfo.operations);
          }
        },
        this.flowController);
  }

  public void handleFailedOperations(List<? extends Row> operations) {
    // TODO(aczajkowski): call write error handler.
  }

  @Override
  public TableName getName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Configuration getConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getRpcTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getReadRpcTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getOperationTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getScanTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] bytes, byte[] bytes1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void scan(Scan scan, C c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultScanner getScanner(Scan scan) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<List<Result>> scanAll(Scan scan) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CompletableFuture<Result>> get(List<Get> list) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CompletableFuture<Void>> put(List<Put> list) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> list) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> list) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(
      Function<RpcChannel, S> function, ServiceCaller<S, R> serviceCaller, byte[] bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <S, R> CoprocessorServiceBuilder<S, R> coprocessorService(
      Function<RpcChannel, S> function,
      ServiceCaller<S, R> serviceCaller,
      CoprocessorCallback<R> coprocessorCallback) {
    throw new UnsupportedOperationException();
  }
}

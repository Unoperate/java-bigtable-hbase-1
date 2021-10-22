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

import static com.google.cloud.bigtable.mirroring.hbase2_x.utils.AsyncRequestScheduling.reserveFlowControlResourcesThenScheduleSecondary;

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringTable;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers.FailedSuccessfulSplit;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers.ReadWriteSplit;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.cloud.bigtable.mirroring.hbase2_x.utils.AsyncRequestScheduling.ResultWithVerificationCompletion;
import com.google.cloud.bigtable.mirroring.hbase2_x.utils.futures.FutureConverter;
import com.google.cloud.bigtable.mirroring.hbase2_x.utils.futures.FutureUtils;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.FutureCallback;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private final Predicate<Object> resultIsFaultyPredicate = (o) -> o instanceof Throwable;
  private final AsyncTable<C> primaryTable;
  private final AsyncTable<C> secondaryTable;
  private final VerificationContinuationFactory verificationContinuationFactory;
  private final FlowController flowController;
  private final SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;
  private final MirroringTracer mirroringTracer;
  private final ListenableReferenceCounter referenceCounter;
  private final ReadSampler readSampler;

  public MirroringAsyncTable(
      AsyncTable<C> primaryTable,
      AsyncTable<C> secondaryTable,
      MismatchDetector mismatchDetector,
      FlowController flowController,
      SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer,
      MirroringTracer mirroringTracer,
      ReadSampler readSampler,
      ListenableReferenceCounter referenceCounter) {
    this.primaryTable = primaryTable;
    this.secondaryTable = secondaryTable;
    this.verificationContinuationFactory = new VerificationContinuationFactory(mismatchDetector);
    this.flowController = flowController;
    this.secondaryWriteErrorConsumer = secondaryWriteErrorConsumer;
    this.mirroringTracer = mirroringTracer;
    this.referenceCounter = referenceCounter;
    this.readSampler = readSampler;
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    CompletableFuture<Result> primaryFuture = this.primaryTable.get(get);
    ResultWithVerificationCompletion<CompletableFuture<Result>> completionStages =
        readWithVerificationAndFlowControl(
            RequestResourcesDescription::new,
            primaryFuture,
            () -> this.secondaryTable.get(get),
            (result) -> this.verificationContinuationFactory.get(get, result));
    keepReferenceUntilOperationCompletes(completionStages.getVerificationCompletedFuture());
    return completionStages.result;
  }

  @Override
  public CompletableFuture<Boolean> exists(Get get) {
    CompletableFuture<Boolean> primaryFuture = this.primaryTable.exists(get);
    ResultWithVerificationCompletion<CompletableFuture<Boolean>> completionStages =
        readWithVerificationAndFlowControl(
            RequestResourcesDescription::new,
            primaryFuture,
            () -> this.secondaryTable.exists(get),
            (result) -> this.verificationContinuationFactory.exists(get, result));
    keepReferenceUntilOperationCompletes(completionStages.getVerificationCompletedFuture());
    return completionStages.result;
  }

  @Override
  public CompletableFuture<Void> put(Put put) {
    CompletableFuture<Void> primaryFuture = this.primaryTable.put(put);
    ResultWithVerificationCompletion<CompletableFuture<Void>> completionStages =
        writeWithFlowControl(
            new MirroringTable.WriteOperationInfo(put),
            primaryFuture,
            () -> this.secondaryTable.put(put));
    keepReferenceUntilOperationCompletes(completionStages.getVerificationCompletedFuture());
    return completionStages.result;
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    CompletableFuture<Void> primaryFuture = this.primaryTable.delete(delete);
    ResultWithVerificationCompletion<CompletableFuture<Void>> completionStages =
        writeWithFlowControl(
            new MirroringTable.WriteOperationInfo(delete),
            primaryFuture,
            () -> this.secondaryTable.delete(delete));
    keepReferenceUntilOperationCompletes(completionStages.getVerificationCompletedFuture());
    return completionStages.result;
  }

  @Override
  public CompletableFuture<Result> append(Append append) {
    CompletableFuture<Result> primaryFuture = this.primaryTable.append(append);
    ResultWithVerificationCompletion<CompletableFuture<Result>> completionStages =
        writeWithFlowControl(
            new MirroringTable.WriteOperationInfo(append),
            primaryFuture,
            () -> this.secondaryTable.append(append));
    keepReferenceUntilOperationCompletes(completionStages.getVerificationCompletedFuture());
    return completionStages.result;
  }

  @Override
  public CompletableFuture<Result> increment(Increment increment) {
    CompletableFuture<Result> primaryFuture = this.primaryTable.increment(increment);
    ResultWithVerificationCompletion<CompletableFuture<Result>> completionStages =
        writeWithFlowControl(
            new MirroringTable.WriteOperationInfo(increment),
            primaryFuture,
            () -> this.secondaryTable.increment(increment));
    keepReferenceUntilOperationCompletes(completionStages.getVerificationCompletedFuture());
    return completionStages.result;
  }

  @Override
  public CompletableFuture<Void> mutateRow(RowMutations rowMutations) {
    CompletableFuture<Void> primaryFuture = this.primaryTable.mutateRow(rowMutations);
    ResultWithVerificationCompletion<CompletableFuture<Void>> completionStages =
        writeWithFlowControl(
            new MirroringTable.WriteOperationInfo(rowMutations),
            primaryFuture,
            () -> this.secondaryTable.mutateRow(rowMutations));
    keepReferenceUntilOperationCompletes(completionStages.getVerificationCompletedFuture());
    return completionStages.result;
  }

  @Override
  public List<CompletableFuture<Result>> get(List<Get> list) {
    ResultWithVerificationCompletion<List<CompletableFuture<Result>>> completionStages =
        generalBatch(
            list,
            this.primaryTable::get,
            this.secondaryTable::get,
            BatchBuilder<Get, Result>::new,
            Result.class);
    keepReferenceUntilOperationCompletes(completionStages.getVerificationCompletedFuture());
    return completionStages.result;
  }

  @Override
  public List<CompletableFuture<Void>> put(List<Put> list) {
    ResultWithVerificationCompletion<List<CompletableFuture<Void>>> completionStages =
        generalBatch(
            list,
            this.primaryTable::put,
            this.secondaryTable::put,
            BatchBuilder<Put, Void>::new,
            Void.class);
    keepReferenceUntilOperationCompletes(completionStages.getVerificationCompletedFuture());
    return completionStages.result;
  }

  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> list) {
    ResultWithVerificationCompletion<List<CompletableFuture<Void>>> completionStages =
        generalBatch(
            list,
            this.primaryTable::delete,
            this.secondaryTable::delete,
            BatchBuilder<Delete, Void>::new,
            Void.class);
    keepReferenceUntilOperationCompletes(completionStages.getVerificationCompletedFuture());
    return completionStages.result;
  }

  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> actions) {
    ResultWithVerificationCompletion<List<CompletableFuture<T>>> completionStages =
        generalBatch(
            actions,
            this.primaryTable::batch,
            this.secondaryTable::batch,
            BatchBuilder<Row, Object>::new,
            Object.class);
    keepReferenceUntilOperationCompletes(completionStages.getVerificationCompletedFuture());
    return completionStages.result;
  }

  @Override
  public List<CompletableFuture<Boolean>> exists(List<Get> list) {
    ResultWithVerificationCompletion<List<CompletableFuture<Boolean>>> completionStages =
        generalBatch(
            list,
            this.primaryTable::exists,
            this.secondaryTable::exists,
            ExistsBuilder::new,
            Boolean.class);
    keepReferenceUntilOperationCompletes(completionStages.getVerificationCompletedFuture());
    return completionStages.result;
  }

  private void keepReferenceUntilOperationCompletes(CompletableFuture<Void> future) {
    this.referenceCounter.incrementReferenceCount();
    future.whenComplete(
        (ignoredResult, ignoredError) -> this.referenceCounter.decrementReferenceCount());
  }

  public <ResultType, ActionType extends Row, SuccessfulResultType>
      ResultWithVerificationCompletion<List<CompletableFuture<ResultType>>> generalBatch(
          List<? extends ActionType> userActions,
          Function<List<ActionType>, List<CompletableFuture<ResultType>>> primaryFunction,
          Function<List<ActionType>, List<CompletableFuture<ResultType>>> secondaryFunction,
          Function<FailedSuccessfulSplit<ActionType, SuccessfulResultType>, GeneralBatchBuilder>
              batchBuilderCreator,
          Class<SuccessfulResultType> successfulResultTypeClass) {
    List<ActionType> actions = new ArrayList<>(userActions);
    final int numActions = actions.size();

    final ResultWithVerificationCompletion<List<CompletableFuture<ResultType>>> returnedValue =
        new ResultWithVerificationCompletion<>(
            generateList(numActions, CompletableFuture<ResultType>::new));

    final List<CompletableFuture<ResultType>> primaryFutures = primaryFunction.apply(actions);
    // Unfortunately, we cannot create ResultType[].
    final Object[] primaryResults = new Object[numActions];

    BiConsumer<Integer, Throwable> primaryErrorHandler =
        (idx, throwable) -> returnedValue.result.get(idx).completeExceptionally(throwable);
    waitForAllWithErrorHandler(primaryFutures, primaryErrorHandler, primaryResults)
        .whenComplete(
            (ignoredResult, ignoredError) -> {
              boolean skipReads = !readSampler.shouldNextReadOperationBeSampled();
              final FailedSuccessfulSplit<ActionType, SuccessfulResultType> failedSuccessfulSplit =
                  BatchHelpers.createOperationsSplit(
                      actions,
                      primaryResults,
                      this.readSampler,
                      resultIsFaultyPredicate,
                      successfulResultTypeClass,
                      skipReads);

              if (failedSuccessfulSplit.successfulOperations.isEmpty()) {
                // Two cases
                // - either everything failed - all results were instances of Throwable and we
                // already completed exceptionally result futures with errorHandler passed to
                // waitForAllWithErrorHandler, or
                // - reads were successful but were excluded due to sampling and we should forward
                // primary results to user results.
                if (skipReads) {
                  completeSuccessfulResultFutures(returnedValue.result, primaryResults, numActions);
                }
                returnedValue.verificationCompleted();
                return;
              }

              GeneralBatchBuilder batchBuilder = batchBuilderCreator.apply(failedSuccessfulSplit);

              final List<ActionType> operationsToScheduleOnSecondary =
                  BatchHelpers.rewriteIncrementsAndAppendsAsPuts(
                      failedSuccessfulSplit.successfulOperations,
                      failedSuccessfulSplit.successfulResults);

              final Object[] secondaryResults = new Object[operationsToScheduleOnSecondary.size()];

              ReadWriteSplit<ActionType, SuccessfulResultType> successfulReadWriteSplit =
                  new ReadWriteSplit<>(
                      failedSuccessfulSplit.successfulOperations,
                      failedSuccessfulSplit.successfulResults,
                      successfulResultTypeClass);

              final RequestResourcesDescription requestResourcesDescription =
                  batchBuilder.getRequestResourcesDescription(operationsToScheduleOnSecondary);

              final CompletableFuture<FlowController.ResourceReservation>
                  resourceReservationRequest =
                      FutureConverter.toCompletable(
                          this.flowController.asyncRequestResource(requestResourcesDescription));

              resourceReservationRequest.whenComplete(
                  (ignoredResourceReservation, resourceReservationError) -> {
                    completeSuccessfulResultFutures(returnedValue.result, primaryResults);
                    if (resourceReservationError != null) {
                      if (!successfulReadWriteSplit.writeOperations.isEmpty()) {
                        secondaryWriteErrorConsumer.consume(
                            HBaseOperation.BATCH, successfulReadWriteSplit.writeOperations);
                      }
                      return;
                    }
                    FutureUtils.forwardResult(
                        reserveFlowControlResourcesThenScheduleSecondary(
                                CompletableFuture.completedFuture(null),
                                resourceReservationRequest,
                                () ->
                                    waitForAllWithErrorHandler(
                                        secondaryFunction.apply(operationsToScheduleOnSecondary),
                                        (idx, throwable) -> {},
                                        secondaryResults),
                                (ignoredPrimaryResult) ->
                                    batchBuilder.getVerificationCallback(secondaryResults))
                            .getVerificationCompletedFuture(),
                        returnedValue.getVerificationCompletedFuture());
                  });
            });
    return returnedValue;
  }

  private <T> ArrayList<T> generateList(int size, Supplier<T> initializer) {
    return Stream.generate(initializer)
        .limit(size)
        .collect(Collectors.toCollection(ArrayList::new));
  }

  private <T> void completeSuccessfulResultFutures(
      List<CompletableFuture<T>> resultFutures, Object[] primaryResults) {
    for (int i = 0; i < primaryResults.length; i++) {
      if (!(resultIsFaultyPredicate.apply(primaryResults[i]))) {
        resultFutures.get(i).complete((T) primaryResults[i]);
      }
    }
  }

  private <T> CompletableFuture<Void> waitForAllWithErrorHandler(
      List<CompletableFuture<T>> futures,
      BiConsumer<Integer, Throwable> errorHandler,
      Object[] results) {
    int numFutures = futures.size();
    List<CompletableFuture<Void>> handledFutures = new ArrayList<>(numFutures);
    for (int i = 0; i < numFutures; i++) {
      final int futureIdx = i;
      handledFutures.add(
          futures
              .get(futureIdx)
              .handle(
                  (result, error) -> {
                    if (error != null) {
                      results[futureIdx] = error;
                      errorHandler.accept(futureIdx, error);
                      throw new CompletionException(error);
                    }
                    results[futureIdx] = result;
                    return null;
                  }));
    }
    return CompletableFuture.allOf(handledFutures.toArray(new CompletableFuture[0]));
  }

  private <T>
      ResultWithVerificationCompletion<CompletableFuture<T>> readWithVerificationAndFlowControl(
          final Function<T, RequestResourcesDescription> resourcesDescriptionCreator,
          final CompletableFuture<T> primaryFuture,
          final Supplier<CompletableFuture<T>> secondaryFutureSupplier,
          final Function<T, FutureCallback<T>> verificationCallbackCreator) {
    ResultWithVerificationCompletion<CompletableFuture<T>> result =
        new ResultWithVerificationCompletion<>(new CompletableFuture<>());
    primaryFuture.whenComplete(
        (primaryResult, primaryError) -> {
          if (primaryError != null) {
            result.result.completeExceptionally(primaryError);
            result.verificationCompleted();
            return;
          }
          if (!this.readSampler.shouldNextReadOperationBeSampled()) {
            result.result.complete(primaryResult);
            result.verificationCompleted();
            return;
          }
          ResultWithVerificationCompletion<CompletableFuture<T>> completionStages =
              reserveFlowControlResourcesThenScheduleSecondary(
                  primaryFuture,
                  FutureConverter.toCompletable(
                      flowController.asyncRequestResource(
                          resourcesDescriptionCreator.apply(primaryResult))),
                  secondaryFutureSupplier,
                  verificationCallbackCreator);
          FutureUtils.forwardResult(completionStages.result, result.result);
          FutureUtils.forwardResult(
              completionStages.getVerificationCompletedFuture(),
              result.getVerificationCompletedFuture());
        });
    return result;
  }

  private <T> ResultWithVerificationCompletion<CompletableFuture<T>> writeWithFlowControl(
      final MirroringTable.WriteOperationInfo writeOperationInfo,
      final CompletableFuture<T> primaryFuture,
      final Supplier<CompletableFuture<T>> secondaryFutureSupplier) {
    final Runnable secondaryWriteErrorHandler =
        () ->
            this.secondaryWriteErrorConsumer.consume(
                writeOperationInfo.hBaseOperation, writeOperationInfo.operations);

    return reserveFlowControlResourcesThenScheduleSecondary(
        primaryFuture,
        FutureConverter.toCompletable(
            flowController.asyncRequestResource(writeOperationInfo.requestResourcesDescription)),
        secondaryFutureSupplier,
        (ignoredSecondaryResult) ->
            new FutureCallback<T>() {
              @Override
              public void onSuccess(@NullableDecl T t) {}

              @Override
              public void onFailure(Throwable throwable) {
                secondaryWriteErrorHandler.run();
              }
            },
        secondaryWriteErrorHandler);
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

  private interface GeneralBatchBuilder {
    RequestResourcesDescription getRequestResourcesDescription(
        List<? extends Row> operationsToPerformOnSecondary);

    FutureCallback<Void> getVerificationCallback(Object[] secondaryResults);
  }

  private class BatchBuilder<ActionType extends Row, SuccessfulResultType>
      implements GeneralBatchBuilder {
    final FailedSuccessfulSplit<ActionType, SuccessfulResultType> failedSuccessfulSplit;
    final ReadWriteSplit<ActionType, Result> successfulReadWriteSplit;

    BatchBuilder(FailedSuccessfulSplit<ActionType, SuccessfulResultType> split) {
      this.failedSuccessfulSplit = split;
      this.successfulReadWriteSplit =
          new ReadWriteSplit<>(
              failedSuccessfulSplit.successfulOperations,
              failedSuccessfulSplit.successfulResults,
              Result.class);
    }

    @Override
    public RequestResourcesDescription getRequestResourcesDescription(
        List<? extends Row> operationsToPerformOnSecondary) {
      return new RequestResourcesDescription(
          operationsToPerformOnSecondary, successfulReadWriteSplit.readResults);
    }

    @Override
    public FutureCallback<Void> getVerificationCallback(Object[] secondaryResults) {
      return BatchHelpers.createBatchVerificationCallback(
          this.failedSuccessfulSplit,
          this.successfulReadWriteSplit,
          secondaryResults,
          verificationContinuationFactory.getMismatchDetector(),
          secondaryWriteErrorConsumer,
          resultIsFaultyPredicate,
          mirroringTracer);
    }
  }

  private class ExistsBuilder implements GeneralBatchBuilder {
    final FailedSuccessfulSplit<Get, Boolean> primaryFailedSuccessfulSplit;
    final boolean[] primarySuccessfulResults;

    ExistsBuilder(FailedSuccessfulSplit<Get, Boolean> split) {
      this.primaryFailedSuccessfulSplit = split;
      this.primarySuccessfulResults =
          new boolean[this.primaryFailedSuccessfulSplit.successfulResults.length];
      for (int i = 0; i < this.primaryFailedSuccessfulSplit.successfulResults.length; i++) {
        this.primarySuccessfulResults[i] = this.primaryFailedSuccessfulSplit.successfulResults[i];
      }
    }

    @Override
    public RequestResourcesDescription getRequestResourcesDescription(
        List<? extends Row> operationsToPerformOnSecondary) {
      return new RequestResourcesDescription(primarySuccessfulResults);
    }

    @Override
    public FutureCallback<Void> getVerificationCallback(Object[] secondaryResults) {
      return new FutureCallback<Void>() {
        @Override
        public void onSuccess(@NullableDecl Void unused) {
          boolean[] booleanSecondaryResults = new boolean[secondaryResults.length];
          for (int i = 0; i < secondaryResults.length; i++) {
            booleanSecondaryResults[i] = (boolean) secondaryResults[i];
          }

          verificationContinuationFactory
              .getMismatchDetector()
              .existsAll(
                  primaryFailedSuccessfulSplit.successfulOperations,
                  primarySuccessfulResults,
                  booleanSecondaryResults);
        }

        @Override
        public void onFailure(Throwable error) {
          verificationContinuationFactory
              .getMismatchDetector()
              .existsAll(primaryFailedSuccessfulSplit.successfulOperations, error);
        }
      };
    }
  }
}

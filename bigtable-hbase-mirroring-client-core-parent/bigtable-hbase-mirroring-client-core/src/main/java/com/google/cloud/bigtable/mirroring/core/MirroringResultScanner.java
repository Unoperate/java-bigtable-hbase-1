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
package com.google.cloud.bigtable.mirroring.core;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.core.MirroringTable.RequestScheduler;
import com.google.cloud.bigtable.mirroring.core.asyncwrappers.AsyncResultScannerWrapper;
import com.google.cloud.bigtable.mirroring.core.asyncwrappers.AsyncResultScannerWrapper.ScannerRequestContext;
import com.google.cloud.bigtable.mirroring.core.utils.AccumulatedExceptions;
import com.google.cloud.bigtable.mirroring.core.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.core.utils.ListenableCloseable;
import com.google.cloud.bigtable.mirroring.core.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.core.verification.VerificationContinuationFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

/**
 * {@link ResultScanner} that performs reads on two underlying tables.
 *
 * <p>Every read is performed synchronously on `ResultScanner` from primary `Table` and then, if it
 * succeeded, asynchronously on secondary `ResultScanner`.
 */
@InternalApi("For internal usage only")
public class MirroringResultScanner extends AbstractClientScanner implements ListenableCloseable {
  private static final Log log = LogFactory.getLog(MirroringResultScanner.class);
  private final MirroringTracer mirroringTracer;

  private final Scan originalScan;
  private final ResultScanner primaryResultScanner;
  private final AsyncResultScannerWrapper secondaryResultScannerWrapper;
  private final VerificationContinuationFactory verificationContinuationFactory;
  private final ListenableReferenceCounter listenableReferenceCounter;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final boolean isVerificationEnabled;
  private final RequestScheduler requestScheduler;
  /**
   * Keeps track of number of entries already read from this scanner to provide context for
   * MismatchDetectors.
   */
  private int readEntries;

  public MirroringResultScanner(
      Scan originalScan,
      ResultScanner primaryResultScanner,
      AsyncResultScannerWrapper secondaryResultScannerWrapper,
      VerificationContinuationFactory verificationContinuationFactory,
      MirroringTracer mirroringTracer,
      boolean isVerificationEnabled,
      RequestScheduler requestScheduler) {
    this.originalScan = originalScan;
    this.primaryResultScanner = primaryResultScanner;
    this.secondaryResultScannerWrapper = secondaryResultScannerWrapper;
    this.verificationContinuationFactory = verificationContinuationFactory;
    this.requestScheduler = requestScheduler;
    this.listenableReferenceCounter = new ListenableReferenceCounter();
    this.readEntries = 0;

    this.listenableReferenceCounter.holdReferenceUntilClosing(this.secondaryResultScannerWrapper);
    this.mirroringTracer = mirroringTracer;
    this.isVerificationEnabled = isVerificationEnabled;
  }

  @Override
  public Result next() throws IOException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.NEXT)) {

      Result result =
          this.mirroringTracer.spanFactory.wrapPrimaryOperation(
              new CallableThrowingIOException<Result>() {
                @Override
                public Result call() throws IOException {
                  return MirroringResultScanner.this.primaryResultScanner.next();
                }
              },
              HBaseOperation.NEXT);

      int startingIndex = this.readEntries;
      this.readEntries += 1;
      ScannerRequestContext context =
          new ScannerRequestContext(
              this.originalScan,
              result,
              startingIndex,
              this.mirroringTracer.spanFactory.getCurrentSpan());

      this.scheduleRequest(
          new RequestResourcesDescription(result),
          this.secondaryResultScannerWrapper.next(context),
          this.verificationContinuationFactory.scannerNext());
      return result;
    }
  }

  @Override
  public Result[] next(final int entriesToRead) throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(HBaseOperation.NEXT_MULTIPLE)) {
      Result[] results =
          this.mirroringTracer.spanFactory.wrapPrimaryOperation(
              new CallableThrowingIOException<Result[]>() {
                @Override
                public Result[] call() throws IOException {
                  return MirroringResultScanner.this.primaryResultScanner.next(entriesToRead);
                }
              },
              HBaseOperation.NEXT_MULTIPLE);

      int startingIndex = this.readEntries;
      this.readEntries += entriesToRead;
      ScannerRequestContext context =
          new ScannerRequestContext(
              this.originalScan,
              results,
              startingIndex,
              entriesToRead,
              this.mirroringTracer.spanFactory.getCurrentSpan());
      this.scheduleRequest(
          new RequestResourcesDescription(results),
          this.secondaryResultScannerWrapper.next(context),
          this.verificationContinuationFactory.scannerNext());
      return results;
    }
  }

  @Override
  public void close() {
    if (this.closed.getAndSet(true)) {
      return;
    }

    AccumulatedExceptions exceptionsList = new AccumulatedExceptions();
    try {
      this.primaryResultScanner.close();
    } catch (RuntimeException e) {
      exceptionsList.add(e);
    } finally {
      try {
        this.asyncClose();
      } catch (RuntimeException e) {
        log.error("Exception while scheduling this.close().", e);
        exceptionsList.add(e);
      }
    }

    exceptionsList.rethrowAsRuntimeExceptionIfCaptured();
  }

  @VisibleForTesting
  ListenableFuture<Void> asyncClose() {
    this.secondaryResultScannerWrapper.asyncClose();
    this.listenableReferenceCounter.decrementReferenceCount();
    return this.listenableReferenceCounter.getOnLastReferenceClosed();
  }

  /**
   * Renews the lease on primary and secondary scanners, synchronously. If any of the {@link
   * ResultScanner#renewLease()} calls returns a {@code false} we return {@code false}. If primary
   * {@code renewLease()} succeeds and secondary fails we still return {@code false} and leave
   * primary with renewed lease because we have no way of cancelling it - we assume that it will be
   * cleaned up after it expires or when scanner is closed.
   *
   * <p>Bigtable client doesn't support this operation and throws {@link
   * UnsupportedOperationException}, but in fact renewing leases is not needed in Bigtable, thus
   * whenever we encounter {@link UnsupportedOperationException} we assume that is was bigtable
   * throwing it and it means that renewing the lease is not needed and we treat as if it returned
   * {@code true}.
   */
  @Override
  public boolean renewLease() {
    try {
      boolean primaryLease = this.primaryResultScanner.renewLease();
      if (!primaryLease) {
        return false;
      }
    } catch (UnsupportedOperationException e) {
      // We assume that UnsupportedOperationExceptions are thrown by Bigtable client and Bigtable
      // doesn't need to renew scanner's leases. We are behaving as if it returned true.
    }

    try {
      // If secondary won't renew the lease we will return false even though primary has managed to
      // renewed its lease. There is no way of cancelling it so we are just leaving it up to HBase
      // to collect if after it expires again.
      // This operation is not asynchronous because we need to forward its result and forward it
      // to the user. Unfortunately we have to wait until mutex guarding the secondaryScanner is
      // released.
      return this.secondaryResultScannerWrapper.renewLease();
    } catch (UnsupportedOperationException e) {
      // We assume that UnsupportedOperationExceptions are thrown by Bigtable client and Bigtable
      // doesn't need to renew scanner's leases. We are behaving as if it returned true.
      return true;
    }
  }

  @Override
  public ScanMetrics getScanMetrics() {
    return this.primaryResultScanner.getScanMetrics();
  }

  private <T> void scheduleRequest(
      RequestResourcesDescription requestResourcesDescription,
      Supplier<ListenableFuture<T>> nextSupplier,
      FutureCallback<T> scannerNext) {
    if (!this.isVerificationEnabled) {
      return;
    }
    this.listenableReferenceCounter.holdReferenceUntilCompletion(
        this.requestScheduler.scheduleRequestWithCallback(
            requestResourcesDescription,
            nextSupplier,
            this.mirroringTracer.spanFactory.wrapReadVerificationCallback(scannerNext)));
  }

  @Override
  public void addOnCloseListener(Runnable listener) {
    this.listenableReferenceCounter
        .getOnLastReferenceClosed()
        .addListener(listener, MoreExecutors.directExecutor());
  }
}

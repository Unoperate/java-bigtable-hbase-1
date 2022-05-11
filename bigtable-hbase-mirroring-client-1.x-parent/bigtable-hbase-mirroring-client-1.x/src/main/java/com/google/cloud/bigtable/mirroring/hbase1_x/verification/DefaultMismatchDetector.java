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
package com.google.cloud.bigtable.mirroring.hbase1_x.verification;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Comparators;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringMetricsRecorder;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

@InternalApi("For internal usage only")
public class DefaultMismatchDetector implements MismatchDetector {
  private final MirroringMetricsRecorder metricsRecorder;

  public DefaultMismatchDetector(MirroringTracer mirroringTracer) {
    this.metricsRecorder = mirroringTracer.metricsRecorder;
  }

  public void exists(Get request, boolean primary, boolean secondary) {
    if (primary != secondary) {
      // TODO: change system.out.println to Log.trace()
      System.out.println("exists mismatch");
      this.metricsRecorder.recordReadMismatches(HBaseOperation.EXISTS, 1);
    }
  }

  @Override
  public void exists(Get request, Throwable throwable) {
    System.out.println("exists failed");
  }

  @Override
  public void existsAll(List<Get> request, boolean[] primary, boolean[] secondary) {
    if (!Arrays.equals(primary, secondary)) {
      System.out.println("existsAll mismatch");
      this.metricsRecorder.recordReadMismatches(HBaseOperation.EXISTS, primary.length);
    }
  }

  @Override
  public void existsAll(List<Get> request, Throwable throwable) {
    System.out.println("existsAll failed");
  }

  public void get(Get request, Result primary, Result secondary) {
    if (!Comparators.resultsEqual(primary, secondary)) {
      System.out.println("get mismatch");
      this.metricsRecorder.recordReadMismatches(HBaseOperation.GET, 1);
    }
  }

  @Override
  public void get(Get request, Throwable throwable) {
    System.out.println("get failed");
  }

  @Override
  public void get(List<Get> request, Result[] primary, Result[] secondary) {
    verifyResults(primary, secondary, "getAll mismatch", HBaseOperation.GET_LIST);
  }

  @Override
  public void get(List<Get> request, Throwable throwable) {
    System.out.println("getAll failed");
  }

  @Override
  public void scannerNext(Scan request, int entriesAlreadyRead, Result primary, Result secondary) {
    if (!Comparators.resultsEqual(primary, secondary)) {
      System.out.println("scan() mismatch");
      this.metricsRecorder.recordReadMismatches(HBaseOperation.NEXT, 1);
    }
  }

  @Override
  public void scannerNext(Scan request, int entriesAlreadyRead, Throwable throwable) {
    System.out.println("scan() failed");
  }

  @Override
  public void scannerNext(
      Scan request, int entriesAlreadyRead, Result[] primary, Result[] secondary) {
    // TODO: try to find all matching elements and report only those that were not successful
    verifyResults(primary, secondary, "scan(i) mismatch", HBaseOperation.NEXT_MULTIPLE);
  }

  @Override
  public void scannerNext(
      Scan request, int entriesAlreadyRead, int entriesRequested, Throwable throwable) {
    System.out.println("scan(i) failed");
  }

  @Override
  public void batch(List<Get> request, Result[] primary, Result[] secondary) {
    verifyResults(primary, secondary, "batch() mismatch", HBaseOperation.BATCH);
  }

  @Override
  public void batch(List<Get> request, Throwable throwable) {
    System.out.println("batch() failed");
  }

  private void verifyResults(
      Result[] primary, Result[] secondary, String errorMessage, HBaseOperation operation) {
    int minLength = Math.min(primary.length, secondary.length);
    int errors = Math.max(primary.length, secondary.length) - minLength;
    for (int i = 0; i < minLength; i++) {
      if (Comparators.resultsEqual(primary[i], secondary[i])) {
        // TODO: We can use code from bigtable client to properly crop row keys which might be very long.
        System.out.println(errorMessage);
        errors++;
      }
    }
    if (errors > 0) {
      this.metricsRecorder.recordReadMismatches(operation, errors);
    }
  }
}

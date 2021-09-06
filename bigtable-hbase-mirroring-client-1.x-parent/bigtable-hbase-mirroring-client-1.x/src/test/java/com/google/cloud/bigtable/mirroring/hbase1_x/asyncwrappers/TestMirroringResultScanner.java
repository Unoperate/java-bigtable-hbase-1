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
package com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringResultScanner;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestMirroringResultScanner {
  @Test
  public void testScannerCloseWhenFirstCloseThrows() {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);

    VerificationContinuationFactory continuationFactoryMock =
        mock(VerificationContinuationFactory.class);
    AsyncResultScannerWrapper secondaryScannerMock = mock(AsyncResultScannerWrapper.class);
    final ResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(), primaryScannerMock, secondaryScannerMock, continuationFactoryMock);

    doThrow(new RuntimeException("first")).when(primaryScannerMock).close();

    Exception thrown =
        assertThrows(
            RuntimeException.class,
            new ThrowingRunnable() {
              @Override
              public void run() {
                mirroringScanner.close();
              }
            });

    verify(primaryScannerMock, times(1)).close();
    verify(secondaryScannerMock, times(1)).close();
    assertThat(thrown).hasMessageThat().contains("first");
  }

  @Test
  public void testScannerCloseWhenSecondCloseThrows() {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);

    VerificationContinuationFactory continuationFactoryMock =
        mock(VerificationContinuationFactory.class);
    AsyncResultScannerWrapper secondaryScannerWrapperMock = mock(AsyncResultScannerWrapper.class);
    final ResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(), primaryScannerMock, secondaryScannerWrapperMock, continuationFactoryMock);

    doThrow(new RuntimeException("second")).when(secondaryScannerWrapperMock).close();

    Exception thrown =
        assertThrows(
            RuntimeException.class,
            new ThrowingRunnable() {
              @Override
              public void run() {
                mirroringScanner.close();
              }
            });

    verify(primaryScannerMock, times(1)).close();
    verify(secondaryScannerWrapperMock, times(1)).close();
    assertThat(thrown).hasMessageThat().contains("second");
  }

  @Test
  public void testScannerCloseWhenBothCloseThrow() {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);

    VerificationContinuationFactory continuationFactoryMock =
        mock(VerificationContinuationFactory.class);
    AsyncResultScannerWrapper secondaryScannerWrapperMock = mock(AsyncResultScannerWrapper.class);

    final ResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(), primaryScannerMock, secondaryScannerWrapperMock, continuationFactoryMock);

    doThrow(new RuntimeException("first")).when(primaryScannerMock).close();
    doThrow(new RuntimeException("second")).when(secondaryScannerWrapperMock).close();

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            new ThrowingRunnable() {
              @Override
              public void run() {
                mirroringScanner.close();
              }
            });

    verify(primaryScannerMock, times(1)).close();
    verify(secondaryScannerWrapperMock, times(1)).close();
    assertThat(thrown).hasMessageThat().contains("first");
    assertThat(thrown.getSuppressed()).hasLength(1);
    assertThat(thrown.getSuppressed()[0]).hasMessageThat().contains("second");
  }
}

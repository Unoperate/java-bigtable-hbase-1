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
package com.google.cloud.bigtable.hbase.mirroring;

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_MISMATCH_DETECTOR_FACTORY_CLASS;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounter;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.SlowMismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase1_x.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestBlocking {
  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();
  @Rule public ExecutorServiceRule executorServiceRule = ExecutorServiceRule.fixedPoolExecutor(3);
  private DatabaseHelpers databaseHelpers =
      new DatabaseHelpers(connectionRule, executorServiceRule);

  @Rule
  public MismatchDetectorCounterRule mismatchDetectorCounterRule =
      new MismatchDetectorCounterRule();

  private static final byte[] columnFamily1 = "cf1".getBytes();
  private static final byte[] qualifier1 = "q1".getBytes();

  private TableName tableName;

  @Before
  public void setUp() throws IOException {
    this.tableName = connectionRule.createTable(columnFamily1);
  }

  @Test
  public void testConnectionCloseBlocksUntilAllRequestsHaveBeenVerified() throws IOException {
    final int numberOfOperations = 9;
    long beforeTableClose;
    long afterTableClose;
    long afterConnectionClose;

    Configuration config = ConfigurationHelper.newConfiguration();
    config.set(
        MIRRORING_MISMATCH_DETECTOR_FACTORY_CLASS, SlowMismatchDetector.Factory.class.getName());
    SlowMismatchDetector.sleepTime = 1000;

    // TODO:
    // - dodajemy event startowy do slowmismatchdetectora.
    // - dodajemy licznik weryfikacji do wykonania tamze, jak osiaga 0 to ustawiamy flage atomic.
    // - podstawiamy za druga baze danych mocka, ktory na gecie zwraca nulla, a na closie sprawdza flage.
    // - odpowiednio dlugi test (kilka sekund) zmniejsza mozliwosc false negativeow.

    // We run 9 operations with 9 corresponding verifications (each taking at least 1000ms) on a
    // threadpool of 3 threads.
    // We expect the whole scenario to take >= 3 seconds and most of this time should be spent in
    // MirroringConnection#close() which is blocking and will wait for all asynchronously scheduled
    // operations.
    try (MirroringConnection connection = databaseHelpers.createConnection(config)) {
      try (Table t = connection.getTable(tableName)) {
        for (int i = 0; i < numberOfOperations; i++) {
          t.get(Helpers.createGet("1".getBytes(), columnFamily1, qualifier1));
        }
        beforeTableClose = System.currentTimeMillis();
      } // Here MirroringTable#close() is performed and it should be non-blocking.
      afterTableClose = System.currentTimeMillis();
    } // Here MirroringConnection#close() is invoked and it should wait until all async operations
    // finish.
    afterConnectionClose = System.currentTimeMillis();

    long tableCloseDuration = afterTableClose - beforeTableClose;
    long connectionCloseDuration = afterConnectionClose - afterTableClose;

    // MirroringTable#close() should be non blocking and fast operation. 500ms seems fair bound for
    // a simple operation, but it might happen that the test thread won't be given a timeslice while
    // in that method for this long. In such a case the test would fail, TODO.
    assertThat(tableCloseDuration).isLessThan(500);
    // MirroringConnection#close() on the other hand should have blocked.
    // The verification takes at least 3000ms, at most 500ms was taken by table's close, thus at
    // least 2000ms
    assertThat(connectionCloseDuration).isGreaterThan(2000);
    // And check that all verification operations that were started have also finished.
    assertThat(MismatchDetectorCounter.getInstance().getVerificationsStartedCounter())
        .isEqualTo(numberOfOperations);
    assertThat(MismatchDetectorCounter.getInstance().getVerificationsFinishedCounter())
        .isEqualTo(numberOfOperations);
  }

  @Test
  public void testSlowVerificationCreateFlowControllerBackpressure() throws IOException {
    Configuration config = ConfigurationHelper.newConfiguration();
    config.set(
        MIRRORING_MISMATCH_DETECTOR_FACTORY_CLASS, SlowMismatchDetector.Factory.class.getName());
    SlowMismatchDetector.sleepTime = 100;
    config.set(MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS, "10");

    byte[] row = "1".getBytes();
    try (MirroringConnection connection = databaseHelpers.createConnection(config)) {
      try (Table table = connection.getTable(tableName)) {
        table.put(Helpers.createPut(row, columnFamily1, qualifier1, "1".getBytes()));
      }
    }

    long startTime;
    long endTime;
    long duration;

    try (MirroringConnection connection = databaseHelpers.createConnection(config)) {
      startTime = System.currentTimeMillis();
      try (Table table = connection.getTable(tableName)) {
        for (int i = 0; i < 1000; i++) {
          table.get(Helpers.createGet(row, columnFamily1, qualifier1));
        }
      }
    }
    endTime = System.currentTimeMillis();
    duration = endTime - startTime;
    // 1000 requests * 100 ms / 10 concurrent requests
    assertThat(duration).isGreaterThan(10000);

    config.set(MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS, "50");
    try (MirroringConnection connection = databaseHelpers.createConnection(config)) {
      startTime = System.currentTimeMillis();
      try (Table table = connection.getTable(tableName)) {
        for (int i = 0; i < 1000; i++) {
          table.get(Helpers.createGet(row, columnFamily1, qualifier1));
        }
      }
    }
    endTime = System.currentTimeMillis();
    duration = endTime - startTime;
    // 1000 requests * 100 ms / 50 concurrent requests
    assertThat(duration).isGreaterThan(2000);

    config.set(MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS, "1000");
    try (MirroringConnection connection = databaseHelpers.createConnection(config)) {
      startTime = System.currentTimeMillis();
      try (Table table = connection.getTable(tableName)) {
        for (int i = 0; i < 1000; i++) {
          table.get(Helpers.createGet(row, columnFamily1, qualifier1));
        }
      }
    }
    endTime = System.currentTimeMillis();
    duration = endTime - startTime;
    // 1000 requests * 100 ms / 1000 concurrent requests
    assertThat(duration).isLessThan(1000);
  }
}

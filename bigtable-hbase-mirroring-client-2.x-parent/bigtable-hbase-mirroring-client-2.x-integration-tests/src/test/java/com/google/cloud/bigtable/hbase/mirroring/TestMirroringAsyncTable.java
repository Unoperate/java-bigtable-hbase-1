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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigtable.hbase.mirroring.utils.AsyncConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.ExecutorServiceRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.mirroring.hbase2_x.MirroringAsyncConnection;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestMirroringAsyncTable {
  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();

  @ClassRule
  public static AsyncConnectionRule asyncConnectionRule = new AsyncConnectionRule(connectionRule);

  @Rule public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  public DatabaseHelpers databaseHelpers = new DatabaseHelpers(connectionRule, executorServiceRule);

  public static final Configuration config = ConfigurationHelper.newConfiguration();

  static final byte[] columnFamily1 = "cf1".getBytes();
  static final byte[] qualifier1 = "cq1".getBytes();

  public static byte[] rowKeyFromId(int id) {
    return Longs.toByteArray(id);
  }

  @Test
  public void testPutAndGet() throws IOException, ExecutionException, InterruptedException {
    int rowId = 42;
    TableName tableName;

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      tableName = connectionRule.createTable(columnFamily1);
      AsyncTable<AdvancedScanResultConsumer> table = asyncConnection.getTable(tableName);
      table.put(Helpers.createPut(rowId, columnFamily1, qualifier1)).get();
    }

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> table = asyncConnection.getTable(tableName);
      boolean result =
          table.exists(Helpers.createGet(rowKeyFromId(rowId), columnFamily1, qualifier1)).get();
      assertThat(result).isTrue();
    }
  }

  @Test
  public void testResultScanner() throws IOException, ExecutionException, InterruptedException {
    Configuration config = ConfigurationHelper.newConfiguration();
    int databaseEntriesCount = 1000;

    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      TableName tableName = connectionRule.createTable(columnFamily1);
      AsyncTable<AdvancedScanResultConsumer> table = asyncConnection.getTable(tableName);

      List<CompletableFuture<Void>> putFutures =
          IntStream.range(0, databaseEntriesCount)
              .mapToObj(i -> table.put(Helpers.createPut(i, columnFamily1, qualifier1)))
              .collect(Collectors.toList());

      CompletableFuture.allOf(putFutures.toArray(new CompletableFuture[0])).get();

      ResultScanner scanner = table.getScanner(columnFamily1);
      assertThat(Iterators.size(scanner.iterator())).isEqualTo(databaseEntriesCount);
    }
  }

  @Test
  public void testPut() throws IOException, ExecutionException, InterruptedException {
    int databaseEntriesCount = 1000;

    final TableName tableName1 = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t1 = asyncConnection.getTable(tableName1);

      List<CompletableFuture<Void>> putFutures =
          IntStream.range(0, databaseEntriesCount)
              .mapToObj(i -> t1.put(Helpers.createPut(i, columnFamily1, qualifier1)))
              .collect(Collectors.toList());

      CompletableFuture.allOf(putFutures.toArray(new CompletableFuture[0])).get();
    }
    databaseHelpers.verifyTableConsistency(tableName1);

    final TableName tableName2 = connectionRule.createTable(columnFamily1);
    try (MirroringAsyncConnection asyncConnection =
        asyncConnectionRule.createAsyncConnection(config)) {
      AsyncTable<AdvancedScanResultConsumer> t1 = asyncConnection.getTable(tableName1);

      List<List<CompletableFuture<Void>>> putFutures = new ArrayList<>();
      int id = 0;
      for (int i = 0; i < 10; i++) {
        List<Put> puts = new ArrayList<>();
        for (int j = 0; j < 100; j++) {
          puts.add(Helpers.createPut(id, columnFamily1, qualifier1));
          id++;
        }
        putFutures.add(t1.put(puts));
      }
      CompletableFuture.allOf(
              putFutures.stream()
                  .flatMap(List::stream)
                  .collect(Collectors.toList())
                  .toArray(new CompletableFuture[0]))
          .get();
    }
    databaseHelpers.verifyTableConsistency(tableName2);
  }
}

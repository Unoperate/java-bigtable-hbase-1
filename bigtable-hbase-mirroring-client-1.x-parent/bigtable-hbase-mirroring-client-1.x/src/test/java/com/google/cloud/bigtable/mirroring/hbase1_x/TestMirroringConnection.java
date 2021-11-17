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

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class TestMirroringConnection {
  @Rule
  public final ExecutorServiceRule executorServiceRule =
      ExecutorServiceRule.singleThreadedExecutor();

  private Configuration createConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set("hbase.client.connection.impl", MirroringConnection.class.getCanonicalName());
    configuration.set(
        MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, TestConnection.class.getCanonicalName());
    configuration.set(
        MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, TestConnection.class.getCanonicalName());
    configuration.set(MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "1");
    configuration.set(MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "2");
    configuration.set(
        "google.bigtable.mirroring.write-error-log.appender.prefix-path", "/tmp/test-");
    configuration.set("google.bigtable.mirroring.write-error-log.appender.max-buffer-size", "1024");
    configuration.set(
        "google.bigtable.mirroring.write-error-log.appender.drop-on-overflow", "false");
    return configuration;
  }

  @Test
  public void testConnectionFactoryCreatesMirroringConnection() throws IOException {
    TestConnection.reset();
    Configuration configuration = createConfiguration();
    Connection connection = ConnectionFactory.createConnection(configuration);
    assertThat(connection).isInstanceOf(MirroringConnection.class);
    assertThat(((MirroringConnection) connection).getPrimaryConnection())
        .isInstanceOf(TestConnection.class);
    assertThat(((MirroringConnection) connection).getSecondaryConnection())
        .isInstanceOf(TestConnection.class);
  }

  @Test
  public void testClose()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    TestConnection.reset();
    Configuration configuration = createConfiguration();

    final MirroringConnection mirroringConnection =
        spy(
            (MirroringConnection)
                ConnectionFactory.createConnection(
                    configuration, executorServiceRule.executorService));
    assertThat(TestConnection.connectionMocks.size()).isEqualTo(2);

    final MirroringTable mirroringTable =
        spy((MirroringTable) mirroringConnection.getTable(TableName.valueOf("test")));
    final MirroringResultScanner mirroringScanner =
        spy((MirroringResultScanner) mirroringTable.getScanner(new Scan()));

    final SettableFuture<Void> unblockSecondaryScanner = SettableFuture.create();
    final SettableFuture<Void> stopBlocking = SettableFuture.create();
    TestHelpers.blockMethodCall(TestConnection.scannerMocks.get(1), unblockSecondaryScanner).next();

    InOrder inOrder =
        Mockito.inOrder(
            TestConnection.scannerMocks.get(1),
            TestConnection.tableMocks.get(1),
            TestConnection.connectionMocks.get(1));

    Thread t =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  mirroringScanner.next();

                  mirroringScanner.close();
                  mirroringTable.close();

                  Thread.sleep(100);
                  stopBlocking.set(null);
                  mirroringConnection.close();
                } catch (Exception e) {
                  assert false;
                }
              }
            });
    t.start();
    stopBlocking.get(50, TimeUnit.SECONDS);
    unblockSecondaryScanner.set(null);

    t.join();

    executorServiceRule.waitForExecutor();

    inOrder.verify(TestConnection.scannerMocks.get(1)).close();
    inOrder.verify(TestConnection.tableMocks.get(1)).close();
    inOrder.verify(TestConnection.connectionMocks.get(1)).close();

    assertThat(mirroringConnection.isClosed()).isTrue();
    verify(TestConnection.connectionMocks.get(0), times(1)).close();
    verify(TestConnection.tableMocks.get(0), times(1)).close();
    verify(TestConnection.scannerMocks.get(0), times(1)).close();
  }

  @Test
  public void testAbortAbortsUnderlyingConnections() throws IOException {
    TestConnection.reset();
    Configuration configuration = createConfiguration();
    Connection connection = ConnectionFactory.createConnection(configuration);

    assertThat(TestConnection.connectionMocks.size()).isEqualTo(2);
    String expectedString = "expected";
    Throwable expectedThrowable = new Exception();
    connection.abort(expectedString, expectedThrowable);
    verify(TestConnection.connectionMocks.get(0), times(1))
        .abort(expectedString, expectedThrowable);
    verify(TestConnection.connectionMocks.get(1), times(1))
        .abort(expectedString, expectedThrowable);
  }
}

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

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowControlStrategy;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncAdminBuilder;
import org.apache.hadoop.hbase.client.AsyncBufferedMutatorBuilder;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.AsyncTableRegionLocator;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Hbck;
import org.apache.hadoop.hbase.client.ScanResultConsumer;
import org.apache.hadoop.hbase.security.User;

public class MirroringAsyncConnection implements AsyncConnection {
  private MirroringAsyncConfiguration configuration;
  private AsyncConnection primaryConnection;
  private AsyncConnection secondaryConnection;
  private final ExecutorService executorService;
  private final MismatchDetector mismatchDetector;
  private final FlowController flowController;

  public MirroringAsyncConnection(
      Configuration conf,
      /**
       * The constructor is passed an AsyncRegion, which is a private interface in
       * org.apache.hadoop.hbase.client. It points the client to appropriate ZooKeeper instances.
       * It's not needed here but as we use ConnectionFactory#createAsyncConnection which will pass
       * it to constructors of underlying connections, we must have this argument.
       */
      Object o,
      String clusterId,
      User user)
      throws ExecutionException, InterruptedException {
    this(conf, null, user);
  }

  public MirroringAsyncConnection(Configuration conf, ExecutorService pool, User user)
      throws ExecutionException, InterruptedException {
    this.configuration = new MirroringAsyncConfiguration(conf);

    this.primaryConnection =
        ConnectionFactory.createAsyncConnection(this.configuration.primaryConfiguration, user)
            .get();
    this.secondaryConnection =
        ConnectionFactory.createAsyncConnection(this.configuration.secondaryConfiguration, user)
            .get();

    if (pool == null) {
      this.executorService = Executors.newCachedThreadPool();
    } else {
      this.executorService = pool;
    }
    this.flowController =
        new FlowController(
            this.<FlowControlStrategy>construct(
                this.configuration.mirroringOptions.flowControllerStrategyClass,
                this.configuration.mirroringOptions));

    this.mismatchDetector = construct(this.configuration.mirroringOptions.mismatchDetectorClass);
  }

  private <T> T construct(String className, Object... params) {
    List<Class<?>> constructorArgs = new ArrayList<>();
    for (Object param : params) {
      constructorArgs.add(param.getClass());
    }
    Constructor<T> constructor =
        getConstructor(className, constructorArgs.toArray(new Class<?>[0]));
    try {
      return constructor.newInstance(params);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> Constructor<T> getConstructor(String className, Class<?>... parameterTypes) {
    try {
      @SuppressWarnings("unchecked")
      Class<T> c = (Class<T>) Class.forName(className);
      return c.getDeclaredConstructor(parameterTypes);
    } catch (ClassNotFoundException | ClassCastException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
  }

  // TODO(aczajkowski): use default method after implementing MirroringAsyncTableBuilder
  @Override
  public AsyncTable<ScanResultConsumer> getTable(TableName tableName, ExecutorService pool) {
    return new MirroringAsyncTable(
        this.primaryConnection.getTable(tableName),
        this.secondaryConnection.getTable(tableName),
        this.mismatchDetector,
        this.flowController);
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator(TableName tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearRegionLocationCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncTableBuilder<AdvancedScanResultConsumer> getTableBuilder(TableName tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncTableBuilder<ScanResultConsumer> getTableBuilder(
      TableName tableName, ExecutorService executorService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder(ExecutorService executorService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(
      TableName tableName, ExecutorService executorService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Hbck> getHbck() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Hbck getHbck(ServerName serverName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();
  }
}

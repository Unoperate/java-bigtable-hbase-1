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

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncBufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class MirroringAsyncBufferedMutator implements AsyncBufferedMutator {

  private final AsyncBufferedMutator primary;
  private final AsyncBufferedMutator secondary;

  public MirroringAsyncBufferedMutator(
      AsyncBufferedMutator primary,
      AsyncBufferedMutator secondary,
      FlowController flowController) {
    this.primary = primary;
    this.secondary = secondary;
  }

  @Override
  public TableName getName() {
    return null;
  }

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public CompletableFuture<Void> mutate(Mutation mutation) {
    CompletableFuture<Void> primaryCompleted = primary.mutate(mutation);
    primaryCompleted.thenRunAsync(() -> secondary.mutate(mutation));

    return primaryCompleted;
  }

  @Override
  public List<CompletableFuture<Void>> mutate(List<? extends Mutation> list) {
    return null;
  }

  @Override
  public void flush() {}

  @Override
  public void close() {}

  @Override
  public long getWriteBufferSize() {
    return 0;
  }

  @Override
  public long getPeriodicalFlushTimeout(TimeUnit unit) {
    return AsyncBufferedMutator.super.getPeriodicalFlushTimeout(unit);
  }
}

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
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase2_x.utils.futures.FutureConverter;
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
  private final FlowController flowController;

  public MirroringAsyncBufferedMutator(
      AsyncBufferedMutator primary,
      AsyncBufferedMutator secondary,
      FlowController flowController) {
    this.primary = primary;
    this.secondary = secondary;
    this.flowController = flowController;
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
    CompletableFuture<Void> resultFuture = new CompletableFuture<>();

    primaryCompleted.thenRunAsync(() -> {

      // when primary completes, request resources.
      CompletableFuture<FlowController.ResourceReservation> resourceAcquired =
              FutureConverter.toCompletable(flowController.asyncRequestResource(new RequestResourcesDescription(mutation)));

      resourceAcquired.thenRunAsync(() -> {
        // got resources, complete resultFuture and schedule secondary
        resultFuture.complete(null);
        secondary.mutate(mutation);
      }).exceptionally(ex -> {
        // got error, complete resultFuture and handle secondary failure
        resultFuture.complete(null);
        System.out.println("secondary failed!");
        return null;
      });
    }).exceptionally(ex -> {
      // primary failed, propagate error
      resultFuture.completeExceptionally(ex);
      return null;
    });

    return resultFuture;
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

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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

public class ReferenceCounterUtils {
  /** Increments the reference counter and decrements it after the future is resolved. */
  public static void holdReferenceUntilCompletion(
      final ReferenceCounter referenceCounter, ListenableFuture<?> future) {
    referenceCounter.incrementReferenceCount();
    future.addListener(
        new Runnable() {
          @Override
          public void run() {
            referenceCounter.decrementReferenceCount();
          }
        },
        MoreExecutors.directExecutor());
  }
}

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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.ListenableReferenceCounter;

/**
 * Objects that can run registered listeners after they are closed. Facilitates reference counting
 * using {@link ListenableReferenceCounter}, objects of classes implementing this interface can be
 * used in {@link ListenableReferenceCounter#holdReferenceUntilClosing(ListenableCloseable)}, the
 * reference is decreased after the referenced object is closed.
 */
public interface ListenableCloseable {
  void addOnCloseListener(Runnable listener);
}

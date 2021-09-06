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

import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

/**
 * Detects mismatches between primary and secondary databases. User can provide own implementation
 * (might be deriving from {@link DefaultMismatchDetector}) to handle only specific cases, such as
 * ignoring timestamp mismatches or disabling verification of scan results.
 */
public interface MismatchDetector {
  void exists(Get request, boolean primary, boolean secondary);

  void exists(Get request, Throwable throwable);

  void existsAll(List<Get> request, boolean[] primary, boolean[] secondary);

  void existsAll(List<Get> request, Throwable throwable);

  void get(Get request, Result primary, Result secondary);

  void get(Get request, Throwable throwable);

  void get(List<Get> request, Result[] primary, Result[] secondary);

  void get(List<Get> request, Throwable throwable);
}

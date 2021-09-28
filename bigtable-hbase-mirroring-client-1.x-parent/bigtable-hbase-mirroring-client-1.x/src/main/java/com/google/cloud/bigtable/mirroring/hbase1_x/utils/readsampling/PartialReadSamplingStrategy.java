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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.readsampling;

public class PartialReadSamplingStrategy implements ReadSamplingStrategy {

  private final float interval;
  private float counter;

  public PartialReadSamplingStrategy(float samplingRate) {
    if (samplingRate <= 0 || samplingRate >= 1) {
      throw new IllegalArgumentException("Invalid verification interval");
    }
    this.interval = 1 / samplingRate;
    this.counter = 0;
  }

  /** Should next read operation be sampled. */
  public boolean next() {
    counter += 1;
    if (counter >= interval) {
      counter -= interval;
      return true;
    }
    return false;
  }
}

/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase.mirroring.utils;

import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.core.verification.MismatchDetector;

public class SlowMismatchDetector extends TestMismatchDetector {
  public static int sleepTime = 1000;

  public SlowMismatchDetector(MirroringTracer tracer) {
    super(tracer);
  }

  @Override
  public void onVerificationStarted() {
    super.onVerificationStarted();
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException ignored) {

    }
  }

  public static class Factory implements MismatchDetector.Factory {
    @Override
    public MismatchDetector create(MirroringTracer mirroringTracer) {
      return new SlowMismatchDetector(mirroringTracer);
    }
  }
}

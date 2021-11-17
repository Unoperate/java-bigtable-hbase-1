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

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;

public class MirroringTable extends com.google.cloud.bigtable.mirroring.hbase1_x.MirroringTable
    implements Table {
  public MirroringTable(
      Table primaryTable,
      Table secondaryTable,
      ExecutorService executorService,
      MismatchDetector mismatchDetector,
      FlowController flowController,
      SecondaryWriteErrorConsumer secondaryWriteErrorConsumer,
      ReadSampler readSampler,
      boolean performWritesConcurrently,
      boolean waitForSecondaryWrites,
      ReferenceCounter connectionReferenceCounter,
      MirroringTracer mirroringTracer) {
    super(
        primaryTable,
        secondaryTable,
        executorService,
        mismatchDetector,
        flowController,
        secondaryWriteErrorConsumer,
        readSampler,
        performWritesConcurrently,
        waitForSecondaryWrites,
        connectionReferenceCounter,
        mirroringTracer);
  }

  @Override
  public TableDescriptor getDescriptor() throws IOException {
    return primaryTable.getDescriptor();
  }

  @Override
  public boolean[] exists(List<Get> gets) throws IOException {
    return existsAll(gets);
  }
}

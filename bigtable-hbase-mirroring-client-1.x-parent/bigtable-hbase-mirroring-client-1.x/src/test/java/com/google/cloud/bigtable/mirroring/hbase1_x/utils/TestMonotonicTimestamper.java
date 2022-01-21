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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.timestamper.MonotonicTimestamper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestMonotonicTimestamper {
  @Test
  public void testFillingPutTimestamps() {
    Put p = new Put("row".getBytes(StandardCharsets.UTF_8));
    p.addColumn("f1".getBytes(), "q1".getBytes(), "v".getBytes());
    p.addColumn("f1".getBytes(), "q2".getBytes(), 123L, "v".getBytes());
    p.addImmutable("f2".getBytes(), "q1".getBytes(), "v".getBytes());
    p.addImmutable("f2".getBytes(), "q2".getBytes(), 123L, "v".getBytes());

    long timestampBefore = System.currentTimeMillis();
    new MonotonicTimestamper().fillTimestamp(p);
    long timestampAfter = System.currentTimeMillis();

    assertThat(p.get("f1".getBytes(), "q1".getBytes()).get(0).getTimestamp())
        .isAtLeast(timestampBefore);
    assertThat(p.get("f1".getBytes(), "q1".getBytes()).get(0).getTimestamp())
        .isAtMost(timestampAfter);

    assertThat(p.get("f1".getBytes(), "q2".getBytes()).get(0).getTimestamp()).isEqualTo(123L);

    assertThat(p.get("f2".getBytes(), "q1".getBytes()).get(0).getTimestamp())
        .isAtLeast(timestampBefore);
    assertThat(p.get("f2".getBytes(), "q1".getBytes()).get(0).getTimestamp())
        .isAtMost(timestampAfter);

    assertThat(p.get("f2".getBytes(), "q2".getBytes()).get(0).getTimestamp()).isEqualTo(123L);
  }

  @Test
  public void testFillingRowMutationsTimestamps() throws IOException {
    RowMutations rm = new RowMutations("row".getBytes());
    Put p = new Put("row".getBytes(StandardCharsets.UTF_8));
    p.addColumn("f1".getBytes(), "q1".getBytes(), "v".getBytes());
    p.addColumn("f1".getBytes(), "q2".getBytes(), 123L, "v".getBytes());
    rm.add(p);

    Delete d = new Delete("row".getBytes(StandardCharsets.UTF_8));
    d.addColumn("f1".getBytes(), "q1".getBytes());
    d.addColumn("f1".getBytes(), "q2".getBytes(), 123L);
    rm.add(d);

    long timestampBefore = System.currentTimeMillis();
    new MonotonicTimestamper().fillTimestamp(rm);
    long timestampAfter = System.currentTimeMillis();

    CellScanner cs0 = rm.getMutations().get(0).cellScanner();
    cs0.advance();
    assertThat(cs0.current().getTimestamp()).isAtLeast(timestampBefore);
    assertThat(cs0.current().getTimestamp()).isAtMost(timestampAfter);
    cs0.advance();
    assertThat(cs0.current().getTimestamp()).isEqualTo(123L);

    CellScanner cs1 = rm.getMutations().get(1).cellScanner();
    cs1.advance();
    assertThat(cs1.current().getTimestamp()).isEqualTo(HConstants.LATEST_TIMESTAMP);
    cs1.advance();
    assertThat(cs1.current().getTimestamp()).isEqualTo(123L);
  }

  @Test
  public void testFillingListOfMutations() throws IOException {
    Put p = new Put("row".getBytes(StandardCharsets.UTF_8));
    p.addColumn("f1".getBytes(), "q1".getBytes(), "v".getBytes());

    Delete d = new Delete("row".getBytes(StandardCharsets.UTF_8));
    d.addColumn("f1".getBytes(), "q1".getBytes());

    Increment i = new Increment("row".getBytes(StandardCharsets.UTF_8));
    i.addColumn("f1".getBytes(), "q1".getBytes(), 1);

    Append a = new Append("row".getBytes(StandardCharsets.UTF_8));
    a.add("f1".getBytes(), "q1".getBytes(), "v".getBytes());

    RowMutations rm = new RowMutations("row".getBytes());

    long timestampBefore = System.currentTimeMillis();
    new MonotonicTimestamper().fillTimestamp(Arrays.asList(p, d, i, a, rm));
    long timestampAfter = System.currentTimeMillis();

    assertThat(p.get("f1".getBytes(), "q1".getBytes()).get(0).getTimestamp())
        .isAtLeast(timestampBefore);
    assertThat(p.get("f1".getBytes(), "q1".getBytes()).get(0).getTimestamp())
        .isAtMost(timestampAfter);

    assertThat(d.getFamilyCellMap().values().iterator().next().get(0).getTimestamp())
        .isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(i.getFamilyCellMap().values().iterator().next().get(0).getTimestamp())
        .isEqualTo(HConstants.LATEST_TIMESTAMP);
    assertThat(a.getFamilyCellMap().values().iterator().next().get(0).getTimestamp())
        .isEqualTo(HConstants.LATEST_TIMESTAMP);
  }

  @Test
  public void testFillingListOfRowMutations() throws IOException {
    Put p = new Put("row".getBytes(StandardCharsets.UTF_8));
    p.addColumn("f1".getBytes(), "q1".getBytes(), "v".getBytes());

    Delete d = new Delete("row".getBytes(StandardCharsets.UTF_8));
    d.addColumn("f1".getBytes(), "q1".getBytes());

    RowMutations rm = new RowMutations("row".getBytes());
    rm.add(p);
    rm.add(d);

    long timestampBefore = System.currentTimeMillis();
    new MonotonicTimestamper().fillTimestamp(Arrays.asList(rm));
    long timestampAfter = System.currentTimeMillis();

    assertThat(p.get("f1".getBytes(), "q1".getBytes()).get(0).getTimestamp())
        .isAtLeast(timestampBefore);
    assertThat(p.get("f1".getBytes(), "q1".getBytes()).get(0).getTimestamp())
        .isAtMost(timestampAfter);
  }
}

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
package com.google.cloud.bigtable.mirroring.core.utils.flowcontrol;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestRequestResourcesDescription {

  @Test
  public void testCalculatingSize() throws IOException {
    // 1 aligned to 8
    assertThat(new RequestResourcesDescription(true).sizeInBytes).isEqualTo(8);
    // 2 aligned to 8
    assertThat(new RequestResourcesDescription(new boolean[] {true, false}).sizeInBytes)
        .isEqualTo(8);
    // 9 aligned to 8
    assertThat(new RequestResourcesDescription(new boolean[9]).sizeInBytes).isEqualTo(16);
    Cell c1 =
        CellUtil.createCell(
            "test1".getBytes(),
            "test2".getBytes(),
            "test3".getBytes(),
            12,
            Type.Put.getCode(),
            "test4".getBytes());
    Cell c2 =
        CellUtil.createCell(
            "testtest1".getBytes(),
            "testtest2".getBytes(),
            "testtest3".getBytes(),
            12,
            Type.Put.getCode(),
            "testtest4".getBytes());
    Cell c3 =
        CellUtil.createCell(
            "testtesttest1".getBytes(),
            "testtesttest2".getBytes(),
            "testtesttest3".getBytes(),
            12,
            Type.Put.getCode(),
            "testtesttest4".getBytes());

    assertThat(new RequestResourcesDescription(Result.create(new Cell[] {c1, c2})).sizeInBytes)
        .isEqualTo(CellUtil.estimatedHeapSizeOf(c1) + CellUtil.estimatedHeapSizeOf(c2));

    assertThat(
            new RequestResourcesDescription(
                    new Result[] {
                      Result.create(new Cell[] {c1, c2}), Result.create(new Cell[] {c2, c3})
                    })
                .sizeInBytes)
        .isEqualTo(
            CellUtil.estimatedHeapSizeOf(c1)
                + 2 * CellUtil.estimatedHeapSizeOf(c2)
                + CellUtil.estimatedHeapSizeOf(c3));

    Delete delete = new Delete("test1".getBytes());
    Delete delete2 = new Delete("test1test2".getBytes());
    assertThat(
            new RequestResourcesDescription(delete2).sizeInBytes
                - new RequestResourcesDescription(delete).sizeInBytes)
        .isAtLeast(4);

    RowMutations rowMutations = new RowMutations("row1".getBytes());
    rowMutations.add(new Delete("row1".getBytes()));
    rowMutations.add(new Delete("row1".getBytes()));

    assertThat(new RequestResourcesDescription(rowMutations).sizeInBytes)
        .isAtLeast(13); // 12 bytes of data + some overhead

    assertThat(new RequestResourcesDescription(Arrays.asList(delete, delete2)).sizeInBytes)
        .isAtLeast(15); // 14 bytes of data + some overhead
  }
}

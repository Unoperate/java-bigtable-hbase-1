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

import com.google.cloud.bigtable.mirroring.core.utils.faillog.DefaultSerializer;
import com.google.cloud.bigtable.mirroring.core.utils.faillog.Serializer;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestDefaultSerializer {
  @Test
  public void testDefaultSerializerWithHBase2x() {
    Serializer serializer = new DefaultSerializer();
    Put put = new Put(new byte[] {'r'});

    for (int i = 0; i < 2; i++) {
      byte[] bytes = Bytes.toBytes(i);
      put.addColumn(
          bytes,
          "foo\nbar\rbaz".getBytes(StandardCharsets.UTF_8),
          12345,
          "asd".getBytes(StandardCharsets.UTF_8));
    }
    put.setDurability(Durability.ASYNC_WAL);
    put.setACL("foo", new Permission(Permission.Action.EXEC));

    serializer.serialize(put, null);

    String serialized;
    try {
      throw new RuntimeException("OMG!");
    } catch (Throwable throwable) {
      serialized = new String(serializer.serialize(put, throwable));
    }
  }
}

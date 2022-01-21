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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.timestamper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;

public class MonotonicTimestamper implements Timestamper {
  private final MonotonicTimer timer = new MonotonicTimer();

  @Override
  public void fillTimestamp(Put put) {
    long timestamp = timer.getCurrentTimeMillis();
    setPutTimestamp(put, timestamp);
  }

  @Override
  public void fillTimestamp(RowMutations rowMutations) {
    fillTimestamp(rowMutations.getMutations());
  }

  @Override
  public void fillTimestamp(Iterable<? extends Row> list) {
    long timestamp = timer.getCurrentTimeMillis();
    setTimestamp(list, timestamp);
  }

  private void setTimestamp(Iterable<? extends Row> list, long timestamp) {
    for (Row mutation : list) {
      setTimestamp(mutation, timestamp);
    }
  }

  private void setTimestamp(Row row, long timestamp) {
    if (row instanceof Put) {
      setPutTimestamp((Put) row, timestamp);
    } else if (row instanceof RowMutations) {
      setTimestamp(((RowMutations) row).getMutations(), timestamp);
    }
    // Bigtable doesn't support timestamps for Increment and Append and only a specific subset of
    // Deletes, let's not modify them.
  }

  private void setPutTimestamp(Put put, long timestamp) {
    for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
      for (Cell cell : entry.getValue()) {
        try {
          if (isTimestampNotSet(cell.getTimestamp())) {
            CellUtil.setTimestamp(cell, timestamp);
          }
        } catch (IOException e) {
          // IOException is thrown when `cell` does not implement `SettableTimestamp` and if it
          // doesn't the we do not have any reliable way for setting the timestamp, thus we are just
          // leaving it as-is.
          // This shouldn't happen for vanilla `Put` instances.
        }
      }
    }
  }

  private boolean isTimestampNotSet(long timestamp) {
    return timestamp == HConstants.LATEST_TIMESTAMP;
  }
}

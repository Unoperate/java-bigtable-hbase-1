package com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;

public class WriteOperationInfo {
  public final RequestResourcesDescription requestResourcesDescription;
  public final List<? extends Row> operations;
  public final HBaseOperation hBaseOperation;

  public WriteOperationInfo(Put operation) {
    this(new RequestResourcesDescription(operation), operation, HBaseOperation.PUT);
  }

  public WriteOperationInfo(Delete operation) {
    this(new RequestResourcesDescription(operation), operation, HBaseOperation.DELETE);
  }

  public WriteOperationInfo(RowMutations operation) {
    this(new RequestResourcesDescription(operation), operation, HBaseOperation.MUTATE_ROW);
  }

  private WriteOperationInfo(
      RequestResourcesDescription requestResourcesDescription,
      Row operation,
      HBaseOperation hBaseOperation) {
    this.requestResourcesDescription = requestResourcesDescription;
    this.operations = Collections.singletonList(operation);
    this.hBaseOperation = hBaseOperation;
  }
}

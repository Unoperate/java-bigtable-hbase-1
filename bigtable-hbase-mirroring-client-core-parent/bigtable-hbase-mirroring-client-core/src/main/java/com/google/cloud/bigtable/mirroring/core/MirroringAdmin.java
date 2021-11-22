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
package com.google.cloud.bigtable.mirroring.core;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Minimal implementation of {@link Admin} required to run YCSB benchmark. Most of the methods will
 * throw {@link UnsupportedOperationException}.
 */
public class MirroringAdmin implements Admin {
  private final Admin primaryAdmin;
  private final Admin secondaryAdmin;

  public MirroringAdmin(Admin primaryAdmin, Admin secondaryAdmin) {
    this.primaryAdmin = primaryAdmin;
    this.secondaryAdmin = secondaryAdmin;
  }

  @Override
  public boolean tableExists(TableName tableName) throws IOException {
    return primaryAdmin.tableExists(tableName) && secondaryAdmin.tableExists(tableName);
  }

  @Override
  public void close() throws IOException {
    try {
      primaryAdmin.close();
    } finally {
      secondaryAdmin.close();
    }
  }

  @Override
  public int getOperationTimeout() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void abort(String s, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isAborted() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Connection getConnection() {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] listTables(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] listTables(Pattern pattern, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] listTables(String s, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TableName[] listTableNames(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TableName[] listTableNames(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TableName[] listTableNames(Pattern pattern, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TableName[] listTableNames(String s, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor getTableDescriptor(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTable(HTableDescriptor hTableDescriptor) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTable(HTableDescriptor hTableDescriptor, byte[] bytes, byte[] bytes1, int i)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTable(HTableDescriptor hTableDescriptor, byte[][] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTableAsync(HTableDescriptor hTableDescriptor, byte[][] bytes)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteTable(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] deleteTables(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void truncateTable(TableName tableName, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void enableTable(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void enableTableAsync(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] enableTables(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void disableTableAsync(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void disableTable(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] disableTables(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isTableEnabled(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isTableAvailable(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isTableAvailable(TableName tableName, byte[][] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Pair<Integer, Integer> getAlterStatus(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Pair<Integer, Integer> getAlterStatus(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addColumn(TableName tableName, HColumnDescriptor hColumnDescriptor)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteColumn(TableName tableName, byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void modifyColumn(TableName tableName, HColumnDescriptor hColumnDescriptor)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void closeRegion(String s, String s1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void closeRegion(byte[] bytes, String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean closeRegionWithEncodedRegionName(String s, String s1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void closeRegion(ServerName serverName, HRegionInfo hRegionInfo) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HRegionInfo> getOnlineRegions(ServerName serverName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flush(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flushRegion(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void compact(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void compactRegion(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void compact(TableName tableName, byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void compactRegion(byte[] bytes, byte[] bytes1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void majorCompact(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void majorCompactRegion(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void majorCompact(TableName tableName, byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void majorCompactRegion(byte[] bytes, byte[] bytes1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void compactRegionServer(ServerName serverName, boolean b)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void move(byte[] bytes, byte[] bytes1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void assign(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unassign(byte[] bytes, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void offline(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setBalancerRunning(boolean b, boolean b1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean balancer() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean balancer(boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isBalancerEnabled() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean normalize() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNormalizerEnabled() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setNormalizerRunning(boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean enableCatalogJanitor(boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int runCatalogScan() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCatalogJanitorEnabled() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean setCleanerChoreRunning(boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean runCleanerChore() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCleanerChoreEnabled() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void mergeRegions(byte[] bytes, byte[] bytes1, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void split(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void splitRegion(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void split(TableName tableName, byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void splitRegion(byte[] bytes, byte[] bytes1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void modifyTable(TableName tableName, HTableDescriptor hTableDescriptor)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void stopMaster() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isMasterInMaintenanceMode() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void stopRegionServer(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ClusterStatus getClusterStatus() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Configuration getConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createNamespace(NamespaceDescriptor namespaceDescriptor) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void modifyNamespace(NamespaceDescriptor namespaceDescriptor) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteNamespace(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public NamespaceDescriptor getNamespaceDescriptor(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] listTableDescriptorsByNamespace(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TableName[] listTableNamesByNamespace(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HRegionInfo> getTableRegions(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] getTableDescriptorsByTableName(List<TableName> list)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public HTableDescriptor[] getTableDescriptors(List<String> list) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean abortProcedure(long l, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ProcedureInfo[] listProcedures() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<Boolean> abortProcedureAsync(long l, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollWALWriter(ServerName serverName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] getMasterCoprocessors() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompactionState getCompactionState(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompactionState getCompactionStateForRegion(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLastMajorCompactionTimestamp(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void snapshot(String s, TableName tableName) throws IOException, IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void snapshot(byte[] bytes, TableName tableName)
      throws IOException, IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void snapshot(String s, TableName tableName, Type type)
      throws IOException, IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void snapshot(SnapshotDescription snapshotDescription)
      throws IOException, IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SnapshotResponse takeSnapshotAsync(SnapshotDescription snapshotDescription)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSnapshotFinished(SnapshotDescription snapshotDescription) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void restoreSnapshot(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void restoreSnapshot(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void restoreSnapshot(byte[] bytes, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void restoreSnapshot(String s, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void restoreSnapshot(String s, boolean b, boolean b1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cloneSnapshot(byte[] bytes, TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cloneSnapshot(String s, TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cloneSnapshot(String s, TableName tableName, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void execProcedure(String s, String s1, Map<String, String> map) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] execProcedureWithRet(String s, String s1, Map<String, String> map)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isProcedureFinished(String s, String s1, Map<String, String> map)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SnapshotDescription> listSnapshots() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SnapshotDescription> listSnapshots(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(String s, String s1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SnapshotDescription> listTableSnapshots(Pattern pattern, Pattern pattern1)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteSnapshot(byte[] bytes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteSnapshot(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteSnapshots(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteSnapshots(Pattern pattern) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteTableSnapshots(String s, String s1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteTableSnapshots(Pattern pattern, Pattern pattern1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setQuota(QuotaSettings quotaSettings) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public QuotaRetriever getQuotaRetriever(QuotaFilter quotaFilter) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CoprocessorRpcChannel coprocessorService() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(ServerName serverName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateConfiguration(ServerName serverName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateConfiguration() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMasterInfoPort() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SecurityCapability> getSecurityCapabilities() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean[] setSplitOrMergeEnabled(
      boolean b, boolean b1, MasterSwitchType... masterSwitchTypes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSplitOrMergeEnabled(MasterSwitchType masterSwitchType) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ServerName> listDeadServers() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ServerName> clearDeadServers(List<ServerName> list) throws IOException {
    throw new UnsupportedOperationException();
  }
}

# Mirroring client
## High level overview
The aim of this project is to provide a drop-in replacement for HBase client that mirrors operations performed on HBase cluster to Bigtable cluster to facilitate migration from HBase to Bigtable.

The client connects two databases, called a primary and a secondary. 
By default operations are performed on the primary database and successful ones are replayed on the secondary asynchronously (this behaviour is configurable).
The client does a best-effort attempt to keep the databases in sync, however it does not ensure consistency. 
When a write to the secondary database fails it is written to a log on disk and the user can replay it manually later. 
Consistency of both databases is verified when reads are performed. A fraction of reads is replayed on secondary database and their content is compared -  mismatches are reported as a log message.
Handling of write errors and read mismatches can be overridden by the user.

HBaseAdmin is not supported.

## Example configuration
This configuration mirrors HBase 1.x (primary database) to a Bigtable instance.
```xml
<configuration>
  <property>
    <name>hbase.client.connection.impl</name>
    <value>com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection</value>
  </property>

  <property>
    <name>google.bigtable.mirroring.primary-client.connection.impl</name>
    <value>com.google.cloud.bigtable.hbase1_x.BigtableConnection</value>
  </property>

  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>zookeeper-url</value>
  </property>

  <property>
    <name>hbase.zookeeper.property.clientPort</name>
    <value>2181</value>
  </property>

  <property>
    <name>google.bigtable.mirroring.secondary-client.connection.impl</name>
    <value>default</value>
  </property>

  <property>
    <name>google.bigtable.project.id</name>
    <value>project-id</value>
  </property>

  <property>
    <name>google.bigtable.instance.id</name>
    <value>instance-id</value>
  </property>
</configuration>
```

For mirroring HBase 2.x to Bigtable the following keys should be used.
```xml
<configuration>
  <property>
    <name>hbase.client.connection.impl</name>
    <value>com.google.cloud.bigtable.mirroring.hbase2_x.MirroringConnection</value>
  </property>

  <property>
    <name>google.bigtable.mirroring.primary-client.connection.impl</name>
    <value>com.google.cloud.bigtable.hbase2_x.BigtableConnection</value>
  </property>

  <property>
    <name>google.bigtable.mirroring.secondary-client.connection.impl</name>
    <value>default</value>
  </property>

  <property>
    <name>google.bigtable.mirroring.primary-client.async.connection.impl</name>
    <value>com.google.cloud.bigtable.hbase2_x.BigtableAsyncConnection</value>
  </property>

  <property>
    <name>google.bigtable.mirroring.secondary-client.async.connection.impl</name>
    <value>default</value>
  </property>
</configuration>
```


## Write modes
### Synchronous
To ensure that writes happened on both databases a synchronous mode can be enabled. In this mode blocking operations block until (or, in HBase 2.x - futures will be resolved after) both databases perform the operation. The user-side code can obtain information about which side failed by inspecting thrown exceptions using `MirroringOperationException#extractMirroringOperationException(Throwable)`.
This mode introduces additional latency.
Set `google.bigtable.mirroring.synchronous-writes` to `true` to enable.

### Concurrent
It is possible to reduce latency of writes to secondary database by enabling concurrent writes mode.
In this mode some of write operations to secondary databases will be performed at the same time as a write to primary. 
This can lead to writes that were successful on secondary but not on primary and create inconsistencies.
Only `Put`, `Delete` and `RowMutations` operations can be performed concurrently. It also applies to contents of `batch()` operations - if a single batch contains any operation of different type it will be performed sequentially. For more details consult `caseats` section.

Concurrent mode can only be used along with synchronous mode.
Set `google.bigtable.mirroring.concurrent-writes` to `true` to enable.

## Read verification
A fraction of reads performed on the primary database is replayed on the secondary database to verify consistency. Fraction of reads verified can be controlled by `google.bigtable.mirroring.read-verification-rate-percent`. Each read operation, be it a single Get, Get with a list, `batch()` of operations that might contain reads, or even `getScanner(Scan)` - either or `Results` returned by that operation (or scanner) are verified with the secondary or none of them. 

## Flow control
To limit number of operations that have not yet been mirrored on the secondary database we've introduced a flow control mechanism that throttles user code performing operations on primary database if the secondary database is slower and is not keeping up.
The same feature is used to limit amount of memory used by pending secondary operations.
Operations are throttled until some operations on secondary database completes (successfully or not) and frees up enough resources.
Synchronous operations are throttled by blocking user code in blocking operations until the operation is started on the secondary,
Asynchronous operations are throttled by delaying completing futures until the same moment.


## Configuration options
- `google.bigtable.mirroring.primary-client.connection.impl` - Key to set to a name of Connection class that should be used to connect to primary database. It is used as hbase.client.connection.impl when creating connection to primary database. Set to `default` to use default HBase connection class. Required.
- `google.bigtable.mirroring.secondary-client.connection.impl` - Key to set to a name of Connection class that should be used to connect to secondary database.  It is used as hbase.client.connection.impl when creating connection to secondary database. Set to an `default` to use default HBase connection class. Required.
- `google.bigtable.mirroring.primary-client.async.connection.impl` - Key to set to a name of Connection class that should be used to connect asynchronously to primary database. It is used as hbase.client.async.connection.impl when creating connection to primary database. Set to `default` to use default HBase connection class. Required when using HBase 2.x.
- `google.bigtable.mirroring.secondary-client.async.connection.impl` - Key to set to a name of Connection class that should be used to connect asynchronously to secondary database. It is used as hbase.client.async.connection.impl when creating connection to secondary database. Set to `default` to use default HBase connection class. Required when using HBase 2.x.
- `google.bigtable.mirroring.primary-client.prefix` - By default all parameters from the Configuration object passed to ConnectionFactory#createConnection are passed to Connection instances. If this key is set, then only parameters that start with given prefix are passed to primary Connection. Use it if primary and secondary connections' configurations share a key that should have different value passed to each of connections, e.g. zookeeper url.  Prefixes should not contain dot at the end. default: empty.
- `google.bigtable.mirroring.secondary-client.prefix` - If this key is set, then only parameters that start with given prefix are passed to secondary Connection. default: empty.
- `google.bigtable.mirroring.mismatch-detector.impl` - Path to class implementing MismatchDetector. default: DefaultMismatchDetector, logs detected mismatches to stdout and reports them as OpenCensus metrics.
- `google.bigtable.mirroring.flow-controller-strategy.impl` - Path to class to be used as FlowControllerStrategy. default: RequestCountingFlowControlStrategy.
- `google.bigtable.mirroring.flow-controller-strategy.max-outstanding-requests` - Maximal number of outstanding secondary database requests before throttling requests to primary database. default: 500.
- `google.bigtable.mirroring.write-error-consumer.impl` - Path to class to be used as consumer for secondary database write errors. default: DefaultSecondaryWriteErrorConsumer, forwards errors to faillog using Appender and Serializer.
- `google.bigtable.mirroring.write-error-log.serializer.impl` - faillog Serializer implementation, responsible for serializing write errors reported by the Logger to binary representation, which is later appended to resulting file by the Appender. default: DefaultSerializer, dumps supplied mutation along with error stacktrace as JSON.
- `google.bigtable.mirroring.write-error-log.appender.impl` - faillog Appender implementation. default: DefaultAppender, writes data serialized by Serializer implementation to file on disk.
- `google.bigtable.mirroring.write-error-log.appender.prefix-path` - used by DefaultAppender, prefix used for generating the name of the log file. Required.
- `google.bigtable.mirroring.write-error-log.appender.max-buffer-size` - used by DefaultAppender, maxBufferSize maximum size of the buffer used for communicating with the thread flushing the data to disk. default: 20971520 bytes (20 MB).
- `google.bigtable.mirroring.write-error-log.appender.drop-on-overflow` - used by DefaultAppender, whether to drop data if the thread flushing the data to disk is not keeping up or to block until it catches up. default: false.
- `google.bigtable.mirroring.read-verification-rate-percent` - Integer value representing percentage of read operations performed on primary database that should be verified against secondary. Each call to `Table#get(Get)`, `Table#get(List)`, `Table#exists(Get)`, `Table#existsAll(List)`, `Table#batch(List, Object[])` (with overloads) and `Table#getScanner(Scan)` (with overloads) is counted as a single operation, independent of size of their arguments and results.  Correct values are a integers ranging from 0 to 100 inclusive. default: 100.
- `google.bigtable.mirroring.buffered-mutator.bytes-to-flush` - Number of bytes that `MirroringBufferedMutator` should buffer before flushing underlying primary BufferedMutator and performing a write to secondary database.  If not set uses the value of `hbase.client.write.buffer`, which by default is 2MB.  When those values are kept in sync, mirroring client should perform flush operation on primary BufferedMutator right after it schedules a new asynchronous write to the database.


## Caveats
### Differences between Bigtable and HBase
There are differences between HBase and Bigtable, please consult [this link](https://cloud.google.com/bigtable/docs/hbase-differences).
Code using this client should be aware of them.
### Mirroring Increments and Appends
`increment` and `append` operations do not allow to specify a timestamp of new version to create. To keep databases consistent the Mirroring Client mirrors these operations as `Put`s inserting return values of these methods. This also applies to `Increment`s and `Append`s performed in `batch()` operation. For reason those operations have to be mirrored sequentially, even if concurrent write mode is enabled.
### Verification of 2.x scans is not performed
`AsyncTable#scan(Scan)` operation results are not verified for consistency with secondary database. Bigtable-HBase client doesn't support AdvancedScanResultConsumer and we would not be able to throttle operations on it, in case where Bigtable would be used as a primary database and the secondary database would be significantly slower.

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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * Failed mutation log.
 *
 * <p>Objects of this class enable persisting failed mutations.
 */
public class FailedMutationLogger implements AutoCloseable {
  private final Serializer serializer;
  private final Appender appender;
  private final MirroringTracer mirroringTracer;

  FailedMutationLogger(MirroringTracer mirroringTracer) throws IOException {
    this(mirroringTracer, "/tmp/hbase_mirroring_client_failed_mutations", 1024 * 1024, false);
  }

  /**
   * Create a logger with a default implementation.
   *
   * <p>The default implementation will write to files with the given prefix. Most of the time, the
   * actual writing should be asynchronous, because the failed mutations are buffered and flushed to
   * disk by another thread.
   *
   * @param mirroringTracer tracer for telemetry
   * @param pathPrefix the prefix of the created log files
   * @param maxBufferSize the maximum amount of log entries kept in memory before flushing to disk
   * @param dropOnOverFlow if this logger is not keeping up with flushing the incoming mutations to
   *     disk (i.e. the buffer is becoming full), this parameter governs what to do with following
   *     mutations; if set to true, they will be dropped; if set to false, the log will block the
   *     thread attempting to write until there some data is flushed to disk
   * @throws IOException on failure to write the log
   */
  FailedMutationLogger(
      MirroringTracer mirroringTracer, String pathPrefix, int maxBufferSize, boolean dropOnOverFlow)
      throws IOException {
    this(
        mirroringTracer,
        new DefaultAppender(pathPrefix, maxBufferSize, dropOnOverFlow),
        new DefaultSerializer());
  }

  /**
   * Create a logger with a user-provided implementation of how to serialize log entries and where
   * to store them.
   *
   * @param mirroringTracer tracer for telemetry
   * @param appender an object responsible for storing log entries
   * @param serializer on object responsible for transforming failed mutations into log entries
   */
  public FailedMutationLogger(
      MirroringTracer mirroringTracer, Appender appender, Serializer serializer) {
    this.mirroringTracer = mirroringTracer;
    this.appender = appender;
    this.serializer = serializer;
  }

  /**
   * Log a failed mutation.
   *
   * <p>This method is thread safe.
   *
   * @param mutation the mutation which failed
   * @param failureCause the cause of the failure
   * @throws InterruptedException in case the appender thread was interrupted
   */
  public void mutationFailed(Mutation mutation, Throwable failureCause)
      throws InterruptedException {
    appender.append(serializer.serialize(mutation, failureCause));
  }

  @Override
  public void close() throws Exception {
    appender.close();
  }
}

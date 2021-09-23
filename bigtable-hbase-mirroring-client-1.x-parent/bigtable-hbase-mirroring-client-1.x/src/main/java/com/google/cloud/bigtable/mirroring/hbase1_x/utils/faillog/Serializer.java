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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

/**
 * Objects of this class should transform a failed mutation into data to be passed to {@link
 * Appender}.
 */
public abstract class Serializer {
  /**
   * Create a failed mutation log entry for a given mutation and a failure cause.
   *
   * <p>This method is thread-safe.
   *
   * @param mutation the failed mutation
   * @param failureCause the cause of failure
   * @return data representing the relevant log entry
   */
  public abstract byte[] serialize(Mutation mutation, Throwable failureCause);

  /**
   * Create a default implementation of a {@link Serializer}.
   *
   * @return an implementation, which encodes the mutation, cause of its failure and the timestamp
   *     into an easily parsable JSON. {@link JsonSerializer} is used under the hood, which allows
   *     the user to later recreate the failed mutations and reapply them.
   */
  public static Serializer CreateDefault() {
    return new DefaultSerializer();
  }
}

/**
 * A default, JSON based implementation of the {@link Serializer}.
 *
 * <p>{@link JsonSerializer} is used under the hood to serialize the mutations. Apart from the
 * mutations, additional context is serialized as well, i.e. timestamp, failure reason, stack trace,
 * row key and mutation type (to make deserializing easier).
 *
 * <p>Every log entry is guaranteed to be a single line.
 */
class DefaultSerializer extends Serializer {
  ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public byte[] serialize(Mutation mutation, Throwable failureCause) {
    ClientProtos.MutationProto.MutationType mutationType;
    LogEntry.OperationType operationType;
    if (mutation instanceof Append) {
      mutationType = ClientProtos.MutationProto.MutationType.APPEND;
      operationType = LogEntry.OperationType.APPEND;
    } else if (mutation instanceof Delete) {
      mutationType = ClientProtos.MutationProto.MutationType.DELETE;
      operationType = LogEntry.OperationType.DELETE;
    } else if (mutation instanceof Increment) {
      mutationType = ClientProtos.MutationProto.MutationType.INCREMENT;
      operationType = LogEntry.OperationType.INCREMENT;
    } else if (mutation instanceof Put) {
      mutationType = ClientProtos.MutationProto.MutationType.PUT;
      operationType = LogEntry.OperationType.PUT;
    } else {
      throw new RuntimeException("Invalid mutation type: " + mutation.getClass().toString());
    }
    try {
      ClientProtos.MutationProto mutationProto =
          (mutationType == ClientProtos.MutationProto.MutationType.INCREMENT)
              ? ProtobufUtil.toMutation(
                  (Increment) mutation, ClientProtos.MutationProto.newBuilder(), 0)
              : ProtobufUtil.toMutation(mutationType, mutation, 0);
      LogEntry logEntry =
          new LogEntry(
              failureCause,
              mutation.getRow(),
              operationType,
              JsonSerializer.getInstance().serialize(mutationProto));
      return (objectMapper.writeValueAsString(logEntry) + '\n').getBytes(StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class LogEntry {
    public Date timestamp;
    public String cause;
    public String stackTrace;
    public byte[] rowKey;
    public OperationType mutationType;
    @com.fasterxml.jackson.annotation.JsonRawValue public String mutation;

    public LogEntry(
        Throwable cause, byte[] rowKey, LogEntry.OperationType mutationType, String mutation) {
      this.timestamp = new Date();
      this.cause = (cause != null) ? cause.getMessage() : null;
      this.stackTrace = (cause != null) ? Throwables.getStackTraceAsString(cause) : null;
      this.rowKey = rowKey;
      this.mutationType = mutationType;
      this.mutation = mutation;
    }

    public enum OperationType {
      PUT("put"),
      INCREMENT("inc"),
      APPEND("append"),
      DELETE("delete");

      private final String name;

      OperationType(String name) {
        this.name = name;
      }
    }
  }
}

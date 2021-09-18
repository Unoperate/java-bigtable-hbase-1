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

import com.google.cloud.bigtable.mirroring.hbase1_x.WriteOperationFutureCallback;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.opencensus.common.Scope;
import io.opencensus.trace.Link;
import io.opencensus.trace.Link.Type;
import io.opencensus.trace.Span;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.io.IOException;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

public class MirroringSpan {
  private static final Tracer tracer = Tracing.getTracer();

  public final Span span;

  public MirroringSpan(String name) {
    span = tracer.spanBuilder(name).startSpan();
  }

  public static Span secondaryOperationsSynchronizationWithoutParent() {
    return tracer.spanBuilder("tableSynchronization").startSpan();
  }

  private SpanBuilder primaryOperationSpanBuilder() {
    return tracer.spanBuilderWithExplicitParent("primary", span);
  }

  public Span primaryOperation() {
    return primaryOperationSpanBuilder().startSpan();
  }

  public <T> T wrapPrimaryOperation(CallableThrowingIOException<T> operationRunner)
      throws IOException {
    try (Scope scope = this.primaryOperationScope()) {
      return operationRunner.call();
    } catch (IOException e) {
      this.endWithError(); // Otherwise will be closed by scheduleVerification
      throw e;
    }
  }

  public void wrapPrimaryOperation(RunnableThrowingIOException operationRunner) throws IOException {
    try (Scope scope = this.primaryOperationScope()) {
      operationRunner.run();
    } catch (IOException e) {
      this.endWithError(); // Otherwise will be closed by scheduleVerification
      throw e;
    }
  }

  //  public void wrapPrimaryOperation(RunnableThrowingIOAndInterruptedException operationRunner)
  //      throws IOException, InterruptedException {
  //    try (Scope scope = this.primaryOperationScope()) {
  //      operationRunner.run();
  //    } catch (IOException | InterruptedException e) {
  //      this.endWithError(); // Otherwise will be closed by scheduleVerification
  //      throw e;
  //    }
  //  }

  public Scope primaryOperationScope() {
    return primaryOperationSpanBuilder().startScopedSpan();
  }

  public Scope secondaryOperationsScope() {
    return tracer.spanBuilderWithExplicitParent("secondary", span).startScopedSpan();
  }

  private SpanBuilder flowControlSpanBuilder() {
    return tracer.spanBuilderWithExplicitParent("flowControl", span);
  }

  public Span flowControl() {
    return flowControlSpanBuilder().startSpan();
  }

  public Scope flowControlScope() {
    return flowControlSpanBuilder().startScopedSpan();
  }

  private SpanBuilder postprocessingScopeBuilder() {
    return tracer.spanBuilderWithExplicitParent("postprocessing", span);
  }

  public Scope postprocessingScope() {
    return postprocessingScopeBuilder().startScopedSpan();
  }

  public Scope verificationScope() {
    return tracer.spanBuilderWithExplicitParent("verification", span).startScopedSpan();
  }

  public Scope writeErrorScope() {
    return tracer.spanBuilderWithExplicitParent("writeErrors", span).startScopedSpan();
  }

  public <T> FutureCallback<T> wrapVerificationCallback(final FutureCallback<T> callback) {
    return new FutureCallback<T>() {
      @Override
      public void onSuccess(@NullableDecl T t) {
        try (Scope scope = MirroringSpan.this.verificationScope()) {
          callback.onSuccess(t);
        }
      }

      @Override
      public void onFailure(Throwable throwable) {
        try (Scope scope = MirroringSpan.this.verificationScope()) {
          callback.onFailure(throwable);
        }
      }
    };
  }

  public <T> WriteOperationFutureCallback<T> wrapWriteOperationCallback(
      final WriteOperationFutureCallback<T> callback) {
    return new WriteOperationFutureCallback<T>() {
      @Override
      public void onFailure(Throwable throwable) {
        try (Scope scope = MirroringSpan.this.writeErrorScope()) {
          callback.onFailure(throwable);
        }
      }
    };
  }

  public void end() {
    span.end();
  }

  public void endWithError() {
    span.setStatus(Status.UNKNOWN);
    span.end();
  }

  public void endWhenCompleted(ListenableFuture<Void> onLastReferenceClosed) {
    onLastReferenceClosed.addListener(
        new Runnable() {
          @Override
          public void run() {
            MirroringSpan.this.end();
          }
        },
        MoreExecutors.directExecutor());
  }

  public void addChild(Span childSpan) {
    // The relationship of the current span relative to the linked span.
    this.span.addLink(Link.fromSpanContext(childSpan.getContext(), Type.PARENT_LINKED_SPAN));
  }
}

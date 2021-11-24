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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class TestListenableReferenceCounter {
  @Test
  public void testFutureIsResolvedWhenCounterReachesZero() {
    Runnable runnable = mock(Runnable.class);

    ListenableReferenceCounter listenableReferenceCounter = new ListenableReferenceCounter();
    listenableReferenceCounter
        .getOnLastReferenceClosed()
        .addListener(runnable, MoreExecutors.directExecutor());

    verify(runnable, never()).run();
    listenableReferenceCounter.incrementReferenceCount(); // 2

    verify(runnable, never()).run();
    listenableReferenceCounter.decrementReferenceCount(); // 1

    verify(runnable, never()).run();
    listenableReferenceCounter.decrementReferenceCount(); // 0
    verify(runnable, times(1)).run();
  }

  @Test
  public void testCounterIsDecrementedWhenReferenceIsDone() {
    ListenableReferenceCounter listenableReferenceCounter = spy(new ListenableReferenceCounter());
    SettableFuture<Void> future = SettableFuture.create();
    verify(listenableReferenceCounter, never()).incrementReferenceCount();
    listenableReferenceCounter.holdReferenceUntilCompletion(future);
    verify(listenableReferenceCounter, times(1)).incrementReferenceCount();
    verify(listenableReferenceCounter, never()).decrementReferenceCount();
    future.set(null);
    verify(listenableReferenceCounter, times(1)).incrementReferenceCount();
    verify(listenableReferenceCounter, times(1)).decrementReferenceCount();
  }

  @Test
  public void testCounterIsDecrementedWhenReferenceThrowsException() {
    ListenableReferenceCounter listenableReferenceCounter = spy(new ListenableReferenceCounter());
    SettableFuture<Void> future = SettableFuture.create();
    verify(listenableReferenceCounter, never()).incrementReferenceCount();
    listenableReferenceCounter.holdReferenceUntilCompletion(future);
    verify(listenableReferenceCounter, times(1)).incrementReferenceCount();
    verify(listenableReferenceCounter, never()).decrementReferenceCount();
    future.setException(new Exception("expected"));
    verify(listenableReferenceCounter, times(1)).incrementReferenceCount();
    verify(listenableReferenceCounter, times(1)).decrementReferenceCount();
  }

  @Test
  public void testHoldReferenceUntilClosing() throws IOException {
    ListenableCloseable listenableCloseable =
        mock(ListenableCloseable.class, withSettings().extraInterfaces(Closeable.class));
    final AtomicReference<Runnable> listener = new AtomicReference<>();

    doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                listener.set((Runnable) invocationOnMock.getArgument(0));
                return null;
              }
            })
        .when(listenableCloseable)
        .addOnCloseListener(any(Runnable.class));
    doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                listener.get().run();
                return null;
              }
            })
        .when((Closeable) listenableCloseable)
        .close();

    ListenableReferenceCounter listenableReferenceCounter = spy(new ListenableReferenceCounter());
    verify(listenableReferenceCounter, never()).incrementReferenceCount();

    listenableReferenceCounter.holdReferenceUntilClosing(listenableCloseable);
    verify(listenableReferenceCounter, times(1)).incrementReferenceCount();
    verify(listenableReferenceCounter, never()).decrementReferenceCount();

    ((Closeable) listenableCloseable).close();
    verify(listenableReferenceCounter, times(1)).incrementReferenceCount();
    verify(listenableReferenceCounter, times(1)).decrementReferenceCount();
  }
}

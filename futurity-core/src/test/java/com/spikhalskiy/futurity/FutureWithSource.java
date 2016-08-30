/*
 * Copyright 2016 The Futurity Project
 *
 * The Futurity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.spikhalskiy.futurity;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.Pack200;

public class FutureWithSource<T> implements Future<T> {
    private final AtomicReference<T> futureResult;
    private final AtomicReference<? extends Throwable> exceptionResult;

    public FutureWithSource(AtomicReference<T> futureResult) {
        this(futureResult, null);
    }

    public FutureWithSource(AtomicReference<T> futureResult, AtomicReference<? extends Throwable> exceptionResult) {
        this.futureResult = futureResult;
        this.exceptionResult = exceptionResult;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return futureResult != null && futureResult.get() != null ||
               exceptionResult != null && exceptionResult.get() != null;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        Throwable t = exceptionResult != null ? exceptionResult.get() : null;
        T value = futureResult != null ? futureResult.get() : null;
        while (t == null && value == null) {
            t = exceptionResult != null ? exceptionResult.get() : null;
            value = futureResult != null ? futureResult.get() : null;
        }

        if (t != null) {
            throw new ExecutionException(t);
        } else {
            return value;
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long startTime = System.currentTimeMillis();
        Throwable t = exceptionResult != null ? exceptionResult.get() : null;
        T value = futureResult != null ? futureResult.get() : null;
        boolean isTimeout = startTime + unit.toMillis(timeout) > System.currentTimeMillis();

        while (value == null && t == null && !isTimeout) {
            t = exceptionResult != null ? exceptionResult.get() : null;
            value = futureResult != null ? futureResult.get() : null;
            isTimeout = startTime + unit.toMillis(timeout) > System.currentTimeMillis();
        }

        if (isTimeout) {
            throw new TimeoutException();
        } else if (t != null) {
            throw new ExecutionException(t);
        } else {
            return value;
        }
    }
}

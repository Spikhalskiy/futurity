/*
 * Copyright 2016 Dmitry Spikhalskiy. All Rights Reserved.
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
 * limitations under the License.
 */
package com.spikhalskiy.futurity;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.Pack200;

public class FutureWithSource<T> implements Future<T> {
    private final AtomicReference<T> futureResult;
    private final AtomicReference<? extends Throwable> exceptionResult;
    private volatile boolean cancelled;

    public FutureWithSource(AtomicReference<T> futureResult) {
        this(futureResult, null);
    }

    public FutureWithSource(AtomicReference<T> futureResult, AtomicReference<? extends Throwable> exceptionResult) {
        this.futureResult = futureResult;
        this.exceptionResult = exceptionResult;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        cancelled = true;
        return true;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean isDone() {
        return futureResult != null && futureResult.get() != null ||
               exceptionResult != null && exceptionResult.get() != null ||
               cancelled;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (TimeoutException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long startTime = System.currentTimeMillis();
        Throwable t = exceptionResult != null ? exceptionResult.get() : null;
        T value = futureResult != null ? futureResult.get() : null;
        boolean isTimeout = startTime + unit.toMillis(timeout) > System.currentTimeMillis();

        while (value == null && t == null && !isTimeout && !cancelled) {
            t = exceptionResult != null ? exceptionResult.get() : null;
            value = futureResult != null ? futureResult.get() : null;
            isTimeout = startTime + unit.toMillis(timeout) > System.currentTimeMillis();
        }

        if (isTimeout) {
            throw new TimeoutException();
        } else if (t != null) {
            throw new ExecutionException(t);
        } else if (cancelled) {
            throw new CancellationException();
        } else {
            return value;
        }
    }
}

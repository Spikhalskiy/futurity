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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class Futurity {
    private Futurity() {}

    /**
     * Wrap a {@code future} with a {@link CompletableFuture} using common settings.<br/>
     * Common {@link FuturityWheel} would be used for scheduling the {@code future} checks.<br/>
     * To change settings of a common {@link FuturityWheel} which is used for wrapping
     * in this method and {@link #shiftWithPoll(Future, long, TimeUnit)} use<br/>
     * <code>Futurity.builder.<br/>
     * ... // configuration<br/>
     * .inject();
     * </code><br/>
     * Default settings are<br/>
     * basicPoolPeriod = 1 ms<br/>
     * tickDuration = 500 microseconds<br/>
     *
     * @param future {@link java.util.concurrent.Future} to wrap. This method doesn't change this future.
     * After wrapping you can continue to use it.
     * @param <V> {@code future} result type
     * @return {@link java.util.concurrent.CompletableFuture} that reflects changes in the {@code future}.
     */
    public static <V> CompletableFuture<V> shift(Future<V> future) {
        return CommonFuturityWheel.getCommon().shift(future);
    }

    /**
     * Wrap a {@code future} with a {@link CompletableFuture} using specified {@code pollDuration} poll interval
     * for checks execution.<br/>
     * Common {@link FuturityWheel} would be used for scheduling the {@code future} checks.<br/>
     * If you need to check your future rare enough - it's better to use this optimized method, which makes possible
     * to don't execute a {@link Future} check each basicPoolPeriod of the common {@link FuturityWheel}.
     *
     * To change settings of the common {@link FuturityWheel} which is used for wrapping
     * in this method and {@link #shift(Future)} use<br/>
     * <code>Futurity.builder.<br/>
     * ... // configuration<br/>
     * .inject();
     * </code><br/>
     * Default settings are<br/>
     * basicPoolPeriod = 1 ms<br/>
     * tickDuration = 500 microseconds<br/>
     *
     * @param future {@link java.util.concurrent.Future} to wrap. This method doesn't change this future.
     * After wrapping you can continue to use it.
     * @param <V> {@code future} result type
     * @return {@link java.util.concurrent.CompletableFuture} that reflects changes in the {@code future}.
     */
    public static <V> CompletableFuture<V> shiftWithPoll(Future<V> future, long pollDuration, TimeUnit unit) {
        return CommonFuturityWheel.getCommon().shiftWithPoll(future, pollDuration, unit);
    }

    /**
     * Create builder that makes it possible to configure a new {@link FuturityWheel} as a separate instance or
     * inject new configuration in a common wheel used for {@link Futurity} static methods.
     * @return {@link FuturityBuilder} instance for configuration
     */
    public static FuturityBuilder builder() {
        return new FuturityBuilder();
    }

    static {
        builder().inject();
    }
}

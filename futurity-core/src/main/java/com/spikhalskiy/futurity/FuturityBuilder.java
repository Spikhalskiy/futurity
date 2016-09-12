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

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Default settings are
 * basicPoolPeriod = 1 ms
 * tickDuration = basicPoolPeriod / 2
 * shutdownDuration = 3 * basicPoolPeriod, but not less than 30 seconds
 */
public class FuturityBuilder {
    //TODO we need some way to shutdown this pool
    final static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private final static long NO_VALUE = -1;

    private long basicPoolPeriodNs = NO_VALUE;
    private long tickDurationNs = NO_VALUE;
    private long shutdownDurationMs = NO_VALUE;

    protected FuturityBuilder() {}

    /**
     * Setup basic pool period which would be used to check futures in configured {@link FuturityWheel} instance
     * @param pollPeriod default delay between checks of {@link java.util.concurrent.Future} states
     * @param unit time unit if the {@code poolPeriod}
     * @return {@code this}
     */
    public FuturityBuilder withBasicPollPeriod(long pollPeriod, TimeUnit unit) {
        this.basicPoolPeriodNs = unit.toNanos(pollPeriod);
        return this;
    }

    /**
     * Setup precision of the underlying {@link com.spikhalskiy.hashedwheeltimer.HashedWheelTimer} which is used when
     * you specify pooling timeout in the
     * {@link com.spikhalskiy.futurity.Futurity#shiftWithPoll(Future, long, TimeUnit)} or
     * {@link com.spikhalskiy.futurity.FuturityWheel#shiftWithPoll(Future, long, TimeUnit)}.
     * Doesn't make too much sense to setup it much less than basic pool period in
     * {@link #withBasicPollPeriod(long, TimeUnit)} because this
     * {@link com.spikhalskiy.hashedwheeltimer.HashedWheelTimer} would be checked once per the basic pool period.
     * But it sometimes make sense to setup to 1/2 or 1/4 of the basic pool period to select time slots more precisely.
     *
     * @param tickDuration distance between time slots using for scheduling tasks with custom poll intervals. Lesser
     * the value - more precise time scheduling would be, but less efficient.
     * @param unit time unit if the {@code poolPeriod}
     * @return {@code this}
     */
    public FuturityBuilder withTickDuration(long tickDuration, TimeUnit unit) {
        this.tickDurationNs = unit.toNanos(tickDuration);
        return this;
    }

    /**
     * This parameter makes sense only if building ends with {@link #inject()}
     * @param shutdownDuration duration that old common wheel has for migration to the new wheel before
     * the full termination
     * @param unit time unit of the {@code shutdownDuration}
     * @return {@code this}
     */
    public FuturityBuilder withShutdownDuration(long shutdownDuration, TimeUnit unit) {
        this.shutdownDurationMs = unit.toMillis(shutdownDuration);
        return this;
    }

    /**
     * Create a new separate instance of {@link FuturityWheel} without touching the current common wheel, which
     * is used in {@link Futurity#shift(Future)} and {@link Futurity#shiftWithPoll(Future, long, TimeUnit)}
     * static methods.
     *
     * @return new empty FuturityWheel instance with internal parameters configured by this builder
     */
    public FuturityWheel separate() {
        if (basicPoolPeriodNs != NO_VALUE) {
            if (tickDurationNs != NO_VALUE) {
                return new FuturityWheel(executorService, basicPoolPeriodNs, tickDurationNs);
            } else {
                return new FuturityWheel(executorService, basicPoolPeriodNs, Math.max(basicPoolPeriodNs/2, 1));
            }
        } else {
            if (tickDurationNs != NO_VALUE) {
                return new FuturityWheel(executorService, TimeUnit.MILLISECONDS.toNanos(1), tickDurationNs);
            } else {
                return new FuturityWheel(executorService,
                                         TimeUnit.MILLISECONDS.toNanos(1), TimeUnit.MILLISECONDS.toNanos(1)/2);
            }
        }
    }

    /**
     * Replace the current common wheel used in {@link Futurity#shift(Future)} and
     * {@link Futurity#shiftWithPoll(Future, long, TimeUnit)} static methods. Old wheel would
     * get a chance to migrate all futures inside to the new created wheel.
     */
    public void inject() {
        FuturityWheel futurity = separate();

        long shutdownDuration = this.shutdownDurationMs;
        if (shutdownDuration == NO_VALUE) {
            FuturityWheel currentFuturityWheel = CommonFuturityWheel.get();
            if (currentFuturityWheel == null) {
                shutdownDuration = 0;
            } else {
                shutdownDuration = Math.max(
                        TimeUnit.NANOSECONDS.toMillis(3 * currentFuturityWheel.basicPoolPeriodNs),
                        TimeUnit.SECONDS.toMillis(30));
            }
        }

        switchCommonFuturity(futurity, shutdownDuration);
    }

    private static void switchCommonFuturity(FuturityWheel newFuturity, long timeoutMs) {
        FuturityWheel oldFuturity = CommonFuturityWheel.get();
        CommonFuturityWheel.replace(newFuturity);
        if (oldFuturity != null) {
            oldFuturity.migrateToAndShutdown(timeoutMs, TimeUnit.MILLISECONDS, newFuturity);
        }
    }

}

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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FuturityBuilder {
    final static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private final static long NO_VALUE = -1;

    private long basicPoolPeriodNs = NO_VALUE;
    private long tickDurationNs = NO_VALUE;
    private long shutdownDurationMs = NO_VALUE;

    protected FuturityBuilder() {}

    public FuturityBuilder withBasicPollPeriod(long pollPeriod, TimeUnit unit) {
        this.basicPoolPeriodNs = unit.toNanos(pollPeriod);
        return this;
    }

    public FuturityBuilder withTickDuration(long tickDuration, TimeUnit unit) {
        this.tickDurationNs = unit.toNanos(tickDuration);
        return this;
    }

    public FuturityBuilder withShutdownDuration(long shutdownDuration, TimeUnit unit) {
        this.shutdownDurationMs = unit.toMillis(shutdownDuration);
        return this;
    }

    public void inject() {
        FuturityWheel futurity;
        if (basicPoolPeriodNs != NO_VALUE) {
            if (tickDurationNs != NO_VALUE) {
                futurity = new FuturityWheel(executorService, basicPoolPeriodNs, tickDurationNs);
            } else {
                futurity = new FuturityWheel(executorService, Math.max(basicPoolPeriodNs/2, 1), tickDurationNs);
            }
        } else {
            if (tickDurationNs != NO_VALUE) {
                futurity = new FuturityWheel(executorService, TimeUnit.MILLISECONDS.toNanos(1), tickDurationNs);
            } else {
                futurity = new FuturityWheel(executorService,
                                             TimeUnit.MILLISECONDS.toNanos(1), TimeUnit.MILLISECONDS.toNanos(1));
            }
        }

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

    static void switchCommonFuturity(FuturityWheel newFuturity, long timeoutMs) {
        FuturityWheel oldFuturity = CommonFuturityWheel.get();
        CommonFuturityWheel.replace(newFuturity);
        if (oldFuturity != null) {
            oldFuturity.migrateToAndShutdown(
                    timeoutMs, newFuturity);
        }
    }

}

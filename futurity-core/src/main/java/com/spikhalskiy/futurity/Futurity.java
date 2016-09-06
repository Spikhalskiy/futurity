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

public class Futurity {
    private static FuturityWheel commonFuturity;

    private Futurity() {
    }

    public static <V> CompletableFuture<V> shift(Future<V> future) {
        return commonFuturity.shift(future);
    }

    //TODO now polling with value less than basicPollDuration and timeTick has not too much sense
    public static <V> CompletableFuture<V> shiftWithPoll(Future<V> future, long pollDuration, TimeUnit timeUnit) {
        return commonFuturity.shiftWithPoll(future, pollDuration, timeUnit);
    }

    public FuturityBuilder builder() {
        return new FuturityBuilder();
    }

    static {
        new FuturityBuilder().inject();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                shutdownOnJVMExit();
            }
        });
    }

    static void switchCommonFuturity(FuturityWheel newFuturity) {
        FuturityWheel oldFuturity = commonFuturity;
        commonFuturity = newFuturity;
        if (oldFuturity != null) {
            //TODO do this in proper way - active tasks should be transferred to new common futurity
            oldFuturity.migrateToAndShutdown(newFuturity);
        }
    }

    private static void shutdownOnJVMExit() {
        commonFuturity.shutdownJVM(200, FuturityBuilder.executorService::shutdown);
    }
}

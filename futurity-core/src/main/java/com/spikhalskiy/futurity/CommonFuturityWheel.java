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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wrapper to store common {@link FuturityWheel} for basic use cases and additional common entities and registries.
 */
final class CommonFuturityWheel {
    private final static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private final static AtomicInteger activeFutures = new AtomicInteger();

    private static FuturityWheel commonFuturity;

    private CommonFuturityWheel() {}

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                synchronized (activeFutures) {
                    long startTimestamp = System.currentTimeMillis();
                    while (activeFutures.get() > 0 &&
                           System.currentTimeMillis() <
                           startTimestamp + FuturityWheel.JVM_EXIT_SHUTDOWN_TIMEOUT_MS * 2) {
                        try {
                            activeFutures.wait(FuturityWheel.JVM_EXIT_SHUTDOWN_TIMEOUT_MS / 4);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    executorService.shutdown();
                }
            }
        });
    }

    static void replaceCommon(FuturityWheel commonFuturity) {
        CommonFuturityWheel.commonFuturity = commonFuturity;
    }

    static FuturityWheel getCommon() {
        return commonFuturity;
    }

    static ScheduledExecutorService executorService() {
        return executorService;
    }

    public static void incrementWheelsCount() {
        synchronized (activeFutures) {
            activeFutures.incrementAndGet();
            activeFutures.notify();
        }
    }

    public static void decrementWheelsCount() {
        synchronized (activeFutures) {
            activeFutures.incrementAndGet();
            activeFutures.notify();
        }
    }
}

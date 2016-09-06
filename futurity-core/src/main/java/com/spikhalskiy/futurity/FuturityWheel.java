/*
 *  Copyright 2016 Dmitry Spikhalskiy. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.spikhalskiy.futurity;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class FuturityWheel {
    private final Wheel wheel;

    public FuturityWheel(ScheduledExecutorService executorService,
                         long basicPoolPeriodNs, long tickDurationNs) {
        wheel = new Wheel(executorService, basicPoolPeriodNs, tickDurationNs, 512);
    }

    public <V> CompletableFuture<V> shift(Future<V> future) {
        CompletableFuture<V> result = trivialCast(future);
        if (result != null) {
            return result;
        }
        result = new CompletableFuture<>();
        wheel.submit(future, result);
        return result;
    }

    //TODO now polling with value less than basicPollDuration and timeTick has not too much sense
    public <V> CompletableFuture<V> shiftWithPoll(Future<V> future, long pollDuration, TimeUnit timeUnit) {
        CompletableFuture<V> result = trivialCast(future);
        if (result != null) {
            return result;
        }
        result = new CompletableFuture<>();
        wheel.submit(future, result, timeUnit.toNanos(pollDuration));
        return result;
    }

    @SuppressWarnings("unchecked")
    private static <V> CompletableFuture<V> trivialCast(Future<V> future) {
        if (future instanceof CompletionStage) {
            if (future instanceof CompletableFuture) {
                return (CompletableFuture<V>)future;
            } else {
                return ((CompletionStage<V>) future).toCompletableFuture();
            }
        }
        return null;
    }

    public void migrateToAndShutdown(FuturityWheel newFuturity) {
        wheel.migrateTo(newFuturity.wheel);
    }

    void shutdownJVM(long hardTimeoutMs, Runnable callback) {
        wheel.shutdownJVM(hardTimeoutMs, callback);
    }
}

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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Futurity {
    private final static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private static Wheel wheel = new Wheel(executorService, TimeUnit.MILLISECONDS.toNanos(1), TimeUnit.MILLISECONDS.toNanos(1), 512);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                wheel.shutdown(200, executorService::shutdown);
            }
        });
    }

    public static <V> CompletableFuture<V> shift(Future<V> future) {
        CompletableFuture<V> result = trivialCast(future);
        if (result != null) {
            return result;
        }
        result = new CompletableFuture<>();
        wheel.submit(future, result);
        return result;
    }

    //TODO now polling with value less than basicPollDuration and timeTick has not too much sense
    public static <V> CompletableFuture<V> shiftWithPoll(Future<V> future, long pollDuration, TimeUnit timeUnit) {
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
}

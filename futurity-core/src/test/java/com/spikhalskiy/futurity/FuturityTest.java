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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class FuturityTest {
    @Test
    public void returnCompletableFutureBack() {
        CompletableFuture origin = new CompletableFuture();
        assertSame(origin, Futurity.shift(origin));
    }

    @Test
    public void trivial() throws InterruptedException {
        AtomicReference<Boolean> futureResult = new AtomicReference<>();
        Future<Boolean> origin = new FutureWithSource<>(futureResult);
        CompletableFuture<Boolean> shift = Futurity.shift(origin);
        assertFalse(shift.isDone());
        futureResult.set(Boolean.TRUE);
        await().pollInterval(1, TimeUnit.MILLISECONDS).pollDelay(0, TimeUnit.MILLISECONDS)
               .atMost(1, TimeUnit.SECONDS).until(shift::isDone);
    }

    @Test(expected = IllegalStateException.class)
    public void passExceptionCorrectly() throws Throwable {
        AtomicReference<Throwable> exceptionResult = new AtomicReference<>();
        Future<Boolean> origin = new FutureWithSource<>(null, exceptionResult);
        CompletableFuture<Boolean> shift = Futurity.shift(origin);
        assertFalse(shift.isDone());
        Throwable ex = new IllegalStateException();
        exceptionResult.set(ex);
        await().pollInterval(1, TimeUnit.MILLISECONDS).pollDelay(0, TimeUnit.MILLISECONDS)
               .atMost(1, TimeUnit.SECONDS).until(shift::isDone);
        try {
            shift.get();
        } catch (ExecutionException e) {
            Throwable ex1 = e.getCause();
            assertSame(ex1, ex);
            throw ex1;
        }
        fail();
    }

    @Test
    public void finalizingALotOfFutures() {
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        try {
            List<CompletableFuture<?>> futures = new ArrayList<>(1000);

            for (int i = 0; i < 1000; i++) {
                futures.add(Futurity.shift(executorService.submit(new Callable<Object>() {
                    private final Random rnd = new Random();

                    @Override
                    public Object call() throws Exception {
                        Thread.sleep(rnd.nextInt(5));
                        return new Object();
                    }
                })));
            }

            CountDownLatch countDownLatch = new CountDownLatch(500);
            futures.forEach(future -> future.thenAccept(i -> countDownLatch.countDown()));
            await().atMost(5, TimeUnit.SECONDS).until(() -> countDownLatch.getCount() == 0);
        } finally {
            executorService.shutdown();
        }
    }
}

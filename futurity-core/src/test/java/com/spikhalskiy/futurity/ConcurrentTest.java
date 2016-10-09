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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertTrue;

public class ConcurrentTest {
    private ListeningExecutorService executor;
    private FuturityWheel futurityWheel;

    @Before
    public void setUp() throws Exception {
        executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1001));
        futurityWheel = Futurity.builder().withBasicPollPeriod(1, TimeUnit.MILLISECONDS).separate();
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdown();
    }

    @Test
    public void testMultiThreadedSubmit() throws InterruptedException, TimeoutException, ExecutionException {
        final int THREADS_COUNT = 50;
        final int COUNT_PER_THREAD = 1000;
        final int CUSTOM_POLL_PERIOD_MS = 5;

        List<List<AtomicReference<Boolean>>> futureSources = new ArrayList<>();
        List<List<FutureWithSource<Boolean>>> futuresToShift = new ArrayList<>();
        List<List<CompletableFuture<Boolean>>> shiftedFutures = new ArrayList<>();

        for (int i = 0; i < THREADS_COUNT; i++) {
            futureSources.add(new ArrayList<>());
            futuresToShift.add(new ArrayList<>());
            shiftedFutures.add(new ArrayList<>());
            for (int j = 0; j < COUNT_PER_THREAD; j++) {
                AtomicReference<Boolean> ref = new AtomicReference<>();
                futureSources.get(i).add(ref);
                futuresToShift.get(i).add(new FutureWithSource<>(ref));
            }
        }

        List<ListenableFuture<Boolean>> submitFutures = new ArrayList<>();
        for (int indx = 0; indx < THREADS_COUNT; indx++) {
            final int i = indx;
            submitFutures.add(executor.submit(() -> {
                List<CompletableFuture<Boolean>> shifted = shiftedFutures.get(i);
                for (int j = 0; j < COUNT_PER_THREAD; j++) {
                    shifted.add(futurityWheel.shift(futuresToShift.get(i).get(j)));
                }
                for (int j = COUNT_PER_THREAD / 2; j < COUNT_PER_THREAD; j++) {
                    shifted.add(futurityWheel.shiftWithPoll(futuresToShift.get(i).get(j),
                                                            CUSTOM_POLL_PERIOD_MS, TimeUnit.MILLISECONDS));
                }
                return true;
            }));
        }

        for (int i = 0; i < THREADS_COUNT; i++) {
            for (int j = 0; j < COUNT_PER_THREAD; j++) {
                futureSources.get(i).get(j).set(Boolean.TRUE);
            }
        }

        Futures.allAsList(submitFutures).get(5, TimeUnit.MILLISECONDS);

        sleep(2 * CUSTOM_POLL_PERIOD_MS);

        for (int i = 0; i < THREADS_COUNT; i++) {
            for (int j = 0; j < COUNT_PER_THREAD; j++) {
                assertTrue("Index " + i + ":" + j + " is not Done", shiftedFutures.get(i).get(j).isDone());
            }
        }
    }
}

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

import com.spikhalskiy.hashedwheeltimer.HashedWheelTimer;
import com.spikhalskiy.hashedwheeltimer.Task;
import com.spikhalskiy.hashedwheeltimer.Timer;
import org.jctools.queues.MpscChunkedArrayQueue;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class Wheel {
    private final static long BASIC_POOLING = -1;
    private final static int MAX_QUEUE_SIZE = 5000;

    private final long basicPoolPeriodNs;
    private final ScheduledFuture<?> scheduledFuture;
    private final HashedWheelTimer timerWheel;
    private final MpscChunkedArrayQueue<WorkTask> taskSubmissions = new MpscChunkedArrayQueue<>(50, MAX_QUEUE_SIZE, true);
    private final MpscChunkedArrayQueue<StateChange> stateChanges = new MpscChunkedArrayQueue<>(5, 10, true);

    private final LinkedList<WorkTask> basicPooling = new LinkedList<>();

    private WheelState state = WheelState.ACTIVE;

    //migration before shutdown
    private Wheel migrationWheel;

    //shutdown
    private long hardShutdownTimestamp;
    private Runnable shutdownCallback;

    Wheel(ScheduledExecutorService scheduledExecutorService,
          long basicPoolPeriodNs, long tickDurationNs, int ticksPerWheel) {
        this.timerWheel = new HashedWheelTimer(tickDurationNs, TimeUnit.NANOSECONDS, ticksPerWheel);
        this.basicPoolPeriodNs = basicPoolPeriodNs;
        this.scheduledFuture = scheduledExecutorService
                .scheduleAtFixedRate(new Work(), basicPoolPeriodNs, basicPoolPeriodNs, TimeUnit.NANOSECONDS);
    }

    <T> void submit(Future<T> future, CompletableFuture<T> completableFuture) {
        submit(future, completableFuture, BASIC_POOLING);
    }

    <T> void submit(Future<T> future, CompletableFuture<T> completableFuture, long poolingNs) {
        WorkTask<T> workTask = new WorkTask<>(future, completableFuture, poolingNs);
        submit(workTask);
    }

    <T> void submit(WorkTask<T> workTask) {
        if (migrationWheel == null) {
            taskSubmissions.relaxedOffer(workTask);
        } else {
            migrationWheel.submit(workTask);
        }
    }

    /**
     * Endpoint to call to make hard shutdown on JVM exiting
     * @param hardTimeoutMs maximum timeout that wheel has for processing of currently scheduled futures
     * @param callback would be called after full shutdown of this {@link Wheel}
     */
    void shutdownJVM(long hardTimeoutMs, Runnable callback) {
        stateChanges.offer(() -> {
            state = WheelState.SHUTDOWN_JVM;
            hardShutdownTimestamp = System.currentTimeMillis() + hardTimeoutMs;
            shutdownCallback = callback;
        });
    }

    /**
     * Migrate tasks to another wheel
     * @param wheel target wheel for migration
     */
    void migrateTo(Wheel wheel) {
        if (wheel == null) {
            throw new NullPointerException("wheel for migration couldn't be null");
        }
        stateChanges.offer(() -> {
            this.state = WheelState.MIGRATING;
            migrationWheel = wheel;
        });
    }

    private class Work implements Runnable {
        @Override
        public void run() {
            executeStateChange();
            manageSubmissions();
            proceedBasicTasks();
            proceedWheelTasks();
            postProcess();
        }

        private void executeStateChange() {
            StateChange stateChange = stateChanges.relaxedPoll();
            if (stateChange != null) {
                stateChange.run();
            }
        }

        private void manageSubmissions() {
            // implement wheel migration code here related to submissions
            if (migrationWheel != null) {
                migrateSubmissions();
            } else {
                WorkTask task = taskSubmissions.relaxedPoll();
                while (task != null) {
                    long poolingNs = task.getPoolingNs();
                    if (poolingNs == BASIC_POOLING) {
                        basicPooling.add(task);
                    } else {
                        long scheduleIntervalNs = Math.max(poolingNs - basicPoolPeriodNs, 0);
                        timerWheel.newTimeout(scheduleIntervalNs, TimeUnit.NANOSECONDS, task);
                    }
                    task = taskSubmissions.relaxedPoll();
                }
            }
        }

        private void proceedBasicTasks() {
            for (Iterator<WorkTask> it = basicPooling.iterator(); it.hasNext();) {
                WorkTask task = it.next();
                if (task.proceed()) {
                    it.remove();
                }
            }
        }

        private void proceedWheelTasks() {
            timerWheel.expireTimers();
        }

        private void postProcess() {
            switch (state) {
            case MIGRATING:
                migrateScheduled();
                state = WheelState.SHUTDOWN;
                break;
            case SHUTDOWN_JVM:
                if (System.currentTimeMillis() >= hardShutdownTimestamp) {
                    finalShutdown();
                }
                break;
            }
        }

        private void finalShutdown() {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }

            if (shutdownCallback != null) {
                shutdownCallback.run();
            }

            Stream.concat(
                        taskSubmissions.stream(),
                        Stream.concat(  basicPooling.stream(),
                                        StreamSupport.stream(timerWheel.scheduled().spliterator(), false)
                                                     .map(task -> (WorkTask)task.getTask()))
                    ).map(WorkTask::getCompletableFuture)
                               .forEach(completableFuture -> completableFuture.completeExceptionally(
                                       new RuntimeException("Futurity that track this task has been shut down")));
        }

        private void migrateSubmissions() {
            taskSubmissions.forEach(migrationWheel::submit);
        }

        private void migrateScheduled() {
            Stream.concat(basicPooling.stream(),
                                  StreamSupport.stream(timerWheel.scheduled().spliterator(), false)
                                               .map(task -> (WorkTask)task.getTask())
                        ).forEach(Wheel.this::migrateWorkTask);
        }
    }

    private void migrateWorkTask(WorkTask task) {
        migrationWheel.submit(task);
    }

    private class WorkTask<T> implements Task {
        private Future<T> future;
        private CompletableFuture<T> completableFuture;
        private long poolingNs;

        WorkTask(Future<T> future, CompletableFuture<T> completableFuture, long poolingNs) {
            this.future = future;
            this.completableFuture = completableFuture;
            this.poolingNs = poolingNs;
        }

        Future<T> getFuture() {
            return future;
        }

        CompletableFuture<T> getCompletableFuture() {
            return completableFuture;
        }

        long getPoolingNs() {
            return poolingNs;
        }

        @Override
        public void run(Timer timer) {
            proceed(timer);
        }

        boolean proceed() {
            return proceed(null);
        }

        /**
         * @return true if done and should be removed from triggering
         */
        boolean proceed(Timer timer) {
            //if future is done we should consider sink timers to some sort of stack for reusing
            if (future.isDone()) {
                try {
                    completableFuture.complete(future.get());
                } catch (InterruptedException e) {
                    completableFuture.completeExceptionally(new RuntimeException("Futurity thread has been stopped"));
                } catch (ExecutionException e) {
                    completableFuture.completeExceptionally(e.getCause());
                }
                return true;
            }

            if (migrationWheel != null) {
                migrateWorkTask(this);
            } else if (timer != null) {
                timerWheel.rescheduleTimeout(poolingNs, TimeUnit.NANOSECONDS, timer);
            }

            return false;
        }
    }

    private interface StateChange extends Runnable {

    }
}

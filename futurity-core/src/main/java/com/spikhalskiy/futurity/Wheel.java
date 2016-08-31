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

import org.agrona.TimerWheel;
import org.agrona.TimerWheel.Timer;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class Wheel {
    private final static long BASIC_POOLING = -1;
    private final long basicPoolPeriodNs;
    private final ScheduledFuture<?> scheduledFuture;
    private final TimerWheel timerWheel;
    private final ManyToOneConcurrentLinkedQueue<Task> submissions = new ManyToOneConcurrentLinkedQueue<>();
    private final LinkedList<Task> basicPooling = new LinkedList<>();

    private boolean shutdown = false;
    private boolean hardShutdown = false;
    private long hardTimeoutTimestamp;
    private Runnable shutdownCallback;

    Wheel(ScheduledExecutorService scheduledExecutorService,
          long basicPoolPeriodNs, long tickDurationNs, int ticksPerWheel) {
        this.timerWheel = new TimerWheel(tickDurationNs, TimeUnit.NANOSECONDS, ticksPerWheel);
        this.basicPoolPeriodNs = basicPoolPeriodNs;
        this.scheduledFuture = scheduledExecutorService
                .scheduleAtFixedRate(new Work(), basicPoolPeriodNs, basicPoolPeriodNs, TimeUnit.NANOSECONDS);
    }

    <T> void submit(Future<T> future, CompletableFuture<T> completableFuture, long poolingNs) {
        submissions.offer(new Task<>(future, completableFuture, poolingNs));
    }

    <T> void submit(Future<T> future, CompletableFuture<T> completableFuture) {
        submissions.offer(new Task<>(future, completableFuture, BASIC_POOLING));
    }

    void shutdown(long hardTimeoutMs, Runnable callback) {
        this.shutdown = true;
        this.hardTimeoutTimestamp = System.currentTimeMillis() + hardTimeoutMs;
        this.shutdownCallback = callback;
    }

    private class Work implements Runnable {
        @Override
        public void run() {
            Task task = submissions.poll();
            while (task != null) {
                long poolingNs = task.getPoolingNs();
                if (poolingNs == BASIC_POOLING) {
                    basicPooling.add(task);
                } else {
                    long scheduleIntervalNs = Math.max(poolingNs - basicPoolPeriodNs, 0);
                    Timer timer = timerWheel.newTimeout(scheduleIntervalNs, TimeUnit.NANOSECONDS, task::proceed);
                    task.setTimer(timer);
                }
                task = submissions.poll();
            }

            if (shutdown && System.currentTimeMillis() >= hardTimeoutTimestamp) {
                hardShutdown = true;
                scheduledFuture.cancel(false);
            }

            for (Iterator<Task> it = basicPooling.iterator(); it.hasNext();) {
                task = it.next();
                if (task.proceed()) {
                    it.remove();
                }
            }
            timerWheel.expireTimers();
            if (hardShutdown) {
                if (shutdownCallback != null) {
                    shutdownCallback.run();
                }
            }
        }
    }

    private class Task<T> {
        private Future<T> future;
        private CompletableFuture<T> completableFuture;
        private long poolingNs;
        private Timer timer;

        Task(Future<T> future, CompletableFuture<T> completableFuture, long poolingNs) {
            this.future = future;
            this.completableFuture = completableFuture;
            this.poolingNs = poolingNs;
        }

        void setTimer(Timer timer) {
            this.timer = timer;
        }

        long getPoolingNs() {
            return poolingNs;
        }

        /**
         *
         * @return true if done
         */
        boolean proceed() {
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

            if (hardShutdown) {
                //TODO reschedule to alive wheel instead
                completableFuture.completeExceptionally(new RuntimeException("Futurity that track this task has been shut down"));
                return true;
            }

            if (poolingNs != BASIC_POOLING) {
                timerWheel.rescheduleTimeout(poolingNs, TimeUnit.NANOSECONDS, timer);
            }

            return false;
        }
    }
}

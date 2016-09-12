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

import com.spikhalskiy.hashedwheeltimer.HashedWheelTimer;
import com.spikhalskiy.hashedwheeltimer.Task;
import com.spikhalskiy.hashedwheeltimer.Timer;
import org.jctools.queues.MpscChunkedArrayQueue;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class FuturityWheel {
    final static long JVM_EXIT_SHUTDOWN_TIMEOUT_MS = 200;

    protected final static long BASIC_POOLING = -1;
    protected final static int MAX_QUEUE_SIZE = 5000;

    protected final long basicPoolPeriodNs;
    protected final ScheduledFuture<?> scheduledFuture;
    protected final HashedWheelTimer timerWheel;
    protected final MpscChunkedArrayQueue<WorkTask>
            taskSubmissions = new MpscChunkedArrayQueue<>(50, MAX_QUEUE_SIZE, true);
    protected final MpscChunkedArrayQueue<StateChange> stateChanges = new MpscChunkedArrayQueue<>(5, 10, true);

    protected final LinkedList<WorkTask> basicPooling = new LinkedList<>();
    protected final Thread jvmShutdownHook;

    protected WheelState state = WheelState.ACTIVE;
    //last timestamp when we can still do shutdown activities
    protected long hardShutdownTimestamp;

    //migration
    protected FuturityWheel migrationWheel;

    protected FuturityWheel(ScheduledExecutorService executorService,
                  long basicPoolPeriodNs, long tickDurationNs) {
        this(executorService, basicPoolPeriodNs, tickDurationNs, 512);
    }

    protected FuturityWheel(ScheduledExecutorService scheduledExecutorService,
          long basicPoolPeriodNs, long tickDurationNs, int ticksPerWheel) {
        this.timerWheel = new HashedWheelTimer(tickDurationNs, TimeUnit.NANOSECONDS, ticksPerWheel);
        this.basicPoolPeriodNs = basicPoolPeriodNs;
        this.scheduledFuture = scheduledExecutorService
                .scheduleAtFixedRate(new Work(), basicPoolPeriodNs, basicPoolPeriodNs, TimeUnit.NANOSECONDS);

        this.jvmShutdownHook = new Thread() {
            @Override
            public void run() {
                shutdownOnJVMExit();
            }
        };
        Runtime.getRuntime().addShutdownHook(this.jvmShutdownHook);
        CommonFuturityWheel.incrementWheelsCount();
    }

    /**
     * Wrap a {@code future} with a {@link CompletableFuture} using {@code this} wheel for scheduling checks.
     *
     * @param future {@link java.util.concurrent.Future} to wrap. This method doesn't change this future.
     * After wrapping you can continue to use it.
     * @param <V> {@code future} result type
     * @return {@link java.util.concurrent.CompletableFuture} that reflects changes in the {@code future}.
     */
    public <V> CompletableFuture<V> shift(Future<V> future) {
        CompletableFuture<V> result = trivialCast(future);
        if (result != null) {
            return result;
        }
        result = new CompletableFuture<>();
        submit(future, result);
        return result;
    }

    /**
     * Wrap a {@code future} with a {@link CompletableFuture} using {@code this} wheel for scheduling checks with
     * specified {@code pollDuration} poll interval.
     *
     * @param future {@link java.util.concurrent.Future} to wrap. This method doesn't change this future.
     * After wrapping you can continue to use it.
     * @param <V> {@code future} result type
     * @return {@link java.util.concurrent.CompletableFuture} that reflects changes in the {@code future}.
     */
    public <V> CompletableFuture<V> shiftWithPoll(Future<V> future, long pollDuration, TimeUnit unit) {
        CompletableFuture<V> result = trivialCast(future);
        if (result != null) {
            return result;
        }
        result = new CompletableFuture<>();
        submit(future, result, unit.toNanos(pollDuration));
        return result;
    }

    /**
     * Terminate the current wheel after passing {code hardTimeoutMs} time
     * @param hardTimeout maximum timeout that wheel has for checking state of scheduled and new submitted futures
     * @param unit time unit of the {@code hardTimeout} parameter
     */
    public void shutdown(long hardTimeout, TimeUnit unit) {
        stateChanges.offer(() -> {
            hardShutdownTimestamp = System.currentTimeMillis() + unit.toMillis(hardTimeout);
            state = WheelState.SHUTDOWN;
        });
    }

    /**
     * Migrate tasks to the another wheel {@code newFuturityWheel} and terminate the current wheel after that
     * and after passing {code hardTimeoutMs} time.
     * @param hardTimeout maximum timeout that wheel has to complete migration of scheduled and new submitted futures
     * @param unit time unit of the {@code hardTimeout} parameter
     * @param newFuturityWheel target wheel for migration
     */
    public void migrateToAndShutdown(long hardTimeout, TimeUnit unit, FuturityWheel newFuturityWheel) {
        if (newFuturityWheel == null) {
            throw new NullPointerException("newFuturityWheel for migration couldn't be null");
        }
        stateChanges.offer(() -> {
            hardShutdownTimestamp = System.currentTimeMillis() + unit.toMillis(hardTimeout);
            migrationWheel = newFuturityWheel;
            state = WheelState.MIGRATING;
        });
    }

    protected <T> void submit(Future<T> future, CompletableFuture<T> completableFuture) {
        submit(future, completableFuture, BASIC_POOLING);
    }

    protected <T> void submit(Future<T> future, CompletableFuture<T> completableFuture, long poolingNs) {
        WorkTask<T> workTask = new WorkTask<>(future, completableFuture, poolingNs);
        submit(workTask);
    }

    protected <T> void submit(WorkTask<T> workTask) {
        if (migrationWheel != null) {
            migrationWheel.submit(workTask);
        } else {
            taskSubmissions.relaxedOffer(workTask);
        }
    }

    private void shutdownOnJVMExit() {
        shutdownOnJVMExit(JVM_EXIT_SHUTDOWN_TIMEOUT_MS);
    }

    /**
     * Endpoint to call to make hard shutdown on JVM exiting
     * @param hardTimeoutMs maximum timeout that wheel has for processing of currently scheduled futures
     */
    private void shutdownOnJVMExit(long hardTimeoutMs) {
        stateChanges.offer(() -> {
            hardShutdownTimestamp = System.currentTimeMillis() + hardTimeoutMs;
            state = WheelState.SHUTDOWN_JVM;
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
                    if (System.currentTimeMillis() >= hardShutdownTimestamp) {
                        finalShutdown();
                    }
                    break;
                case SHUTDOWN:
                    if (System.currentTimeMillis() >= hardShutdownTimestamp) {
                        killScheduledAndSubmissions("Futurity that track this task has been shut down");
                        finalShutdown();
                    }
                    break;
                case SHUTDOWN_JVM:
                    if (System.currentTimeMillis() >= hardShutdownTimestamp) {
                        killScheduledAndSubmissions("Futurity that track this task has been shut down as a result of JVM shutdown");
                        finalShutdown();
                    }
                    break;
            }
        }

        private void finalShutdown() {
            scheduledFuture.cancel(false);

            Runtime.getRuntime().removeShutdownHook(jvmShutdownHook);
            state = WheelState.TERMINATED;
            CommonFuturityWheel.decrementWheelsCount();
        }

        private void killScheduledAndSubmissions(String exceptionMessage) {
            WorkTask task;
            while ((task = taskSubmissions.poll()) != null) {
                migrationWheel.submit(task);
            }
            basicPooling.stream().map(WorkTask::getCompletableFuture).forEach(
                    completableFuture -> completableFuture.completeExceptionally(
                        new IllegalStateException(exceptionMessage)));
            basicPooling.clear();
            for (Timer timer : timerWheel.scheduled()) {
                ((WorkTask)timer.getTask()).getCompletableFuture().completeExceptionally(
                        new IllegalStateException(exceptionMessage));
                timer.cancel();
            }
        }

        private void migrateSubmissions() {
            WorkTask task;
            while ((task = taskSubmissions.poll()) != null) {
                migrationWheel.submit(task);
            }
        }

        private void migrateScheduled() {
            basicPooling.forEach(migrationWheel::submit);
            basicPooling.clear();
            for (Timer timer : timerWheel.scheduled()) {
                migrationWheel.submit((WorkTask)timer.getTask());
                timer.cancel();
            }
        }
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
            //TODO if future is done we should consider sink timers to some sort of stack for reusing
            if (completableFuture.isDone()) {
                return true;
            }

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
                migrationWheel.submit(this);
            } else if (timer != null) {
                timerWheel.rescheduleTimeout(poolingNs, TimeUnit.NANOSECONDS, timer);
            }

            return false;
        }
    }

    @SuppressWarnings("unchecked")
    protected static <V> CompletableFuture<V> trivialCast(Future<V> future) {
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

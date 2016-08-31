package com.spikhalskiy.futurity;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FuturityBuilder {
    final static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private final static long NO_VALUE = -1;

    private long basicPoolPeriodNs = NO_VALUE;
    private long tickDurationNs = NO_VALUE;

    protected FuturityBuilder() {}

    public FuturityBuilder withBasicPollPeriod(long pollPeriod, TimeUnit unit) {
        this.basicPoolPeriodNs = unit.toNanos(pollPeriod);
        return this;
    }

    public FuturityBuilder withTickDurationNs(long tickDuration, TimeUnit unit) {
        this.tickDurationNs = unit.toNanos(tickDuration);
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

        Futurity.switchCommonFuturity(futurity);
    }

}

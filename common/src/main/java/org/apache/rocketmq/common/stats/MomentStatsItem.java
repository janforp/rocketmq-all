package org.apache.rocketmq.common.stats;

import lombok.Getter;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MomentStatsItem {

    @Getter
    private final AtomicLong value = new AtomicLong(0);

    @Getter
    private final String statsName;

    @Getter
    private final String statsKey;

    private final ScheduledExecutorService scheduledExecutorService;

    private final InternalLogger log;

    public MomentStatsItem(String statsName, String statsKey,
            ScheduledExecutorService scheduledExecutorService, InternalLogger log) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
    }

    public void init() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtMinutes();

                    MomentStatsItem.this.value.set(0);
                } catch (Throwable e) {
                }
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 5, TimeUnit.MILLISECONDS);
    }

    public void printAtMinutes() {
        log.info(String.format("[%s] [%s] Stats Every 5 Minutes, Value: %d",
                this.statsName,
                this.statsKey,
                this.value.get()));
    }
}

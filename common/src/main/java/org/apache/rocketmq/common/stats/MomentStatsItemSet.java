package org.apache.rocketmq.common.stats;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MomentStatsItemSet {

    private final ConcurrentMap<String/* key */, MomentStatsItem> statsItemTable =
            new ConcurrentHashMap<String, MomentStatsItem>(128);

    private final String statsName;

    private final ScheduledExecutorService scheduledExecutorService;

    private final InternalLogger log;

    public MomentStatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, InternalLogger log) {
        this.statsName = statsName;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
        this.init();
    }

    public ConcurrentMap<String, MomentStatsItem> getStatsItemTable() {
        return statsItemTable;
    }

    public String getStatsName() {
        return statsName;
    }

    public void init() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtMinutes();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 5, TimeUnit.MILLISECONDS);
    }

    private void printAtMinutes() {
        Iterator<Entry<String, MomentStatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MomentStatsItem> next = it.next();
            next.getValue().printAtMinutes();
        }
    }

    public void setValue(final String statsKey, final int value) {
        MomentStatsItem statsItem = this.getAndCreateStatsItem(statsKey);
        statsItem.getValue().set(value);
    }

    public MomentStatsItem getAndCreateStatsItem(final String statsKey) {
        MomentStatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null == statsItem) {
            statsItem =
                    new MomentStatsItem(this.statsName, statsKey, this.scheduledExecutorService, this.log);
            MomentStatsItem prev = this.statsItemTable.putIfAbsent(statsKey, statsItem);

            if (null != prev) {
                statsItem = prev;
                // statsItem.init();
            }
        }

        return statsItem;
    }
}

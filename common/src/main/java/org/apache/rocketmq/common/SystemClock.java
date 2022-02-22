package org.apache.rocketmq.common;

public class SystemClock {

    public long now() {
        return System.currentTimeMillis();
    }
}
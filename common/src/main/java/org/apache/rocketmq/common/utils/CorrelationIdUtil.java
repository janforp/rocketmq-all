package org.apache.rocketmq.common.utils;

import java.util.UUID;

public class CorrelationIdUtil {

    public static String createCorrelationId() {
        return UUID.randomUUID().toString();
    }
}

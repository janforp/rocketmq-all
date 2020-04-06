package org.apache.rocketmq.tools.admin.api;

public enum TrackType {
    CONSUMED,
    CONSUMED_BUT_FILTERED,
    PULL,
    NOT_CONSUME_YET,
    NOT_ONLINE,
    UNKNOWN
}

package org.apache.rocketmq.client.trace;

import org.apache.rocketmq.common.MixAll;

public class TraceConstants {

    public static final String GROUP_NAME_PREFIX = "_INNER_TRACE_PRODUCER";

    public static final char CONTENT_SPLITOR = (char) 1;

    public static final char FIELD_SPLITOR = (char) 2;

    public static final String TRACE_INSTANCE_NAME = "PID_CLIENT_INNER_TRACE_PRODUCER";

    public static final String TRACE_TOPIC_PREFIX = MixAll.SYSTEM_TOPIC_PREFIX + "TRACE_DATA_";
}

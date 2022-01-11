package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.common.MixAll;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class TransactionalMessageUtil {

    public static final String REMOVETAG = "d";

    public static Charset charset = StandardCharsets.UTF_8;

    public static String buildOpTopic() {
        // RMQ_SYS_TRANS_OP_HALF_TOPIC
        return MixAll.RMQ_SYS_TRANS_OP_HALF_TOPIC;
    }

    public static String buildHalfTopic() {
        return MixAll.RMQ_SYS_TRANS_HALF_TOPIC;
    }

    public static String buildConsumerGroup() {
        return MixAll.CID_SYS_RMQ_TRANS;
    }
}
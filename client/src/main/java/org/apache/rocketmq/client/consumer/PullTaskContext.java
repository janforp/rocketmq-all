package org.apache.rocketmq.client.consumer;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class PullTaskContext {

    private int pullNextDelayTimeMillis = 200;

    private MQPullConsumer pullConsumer;
}

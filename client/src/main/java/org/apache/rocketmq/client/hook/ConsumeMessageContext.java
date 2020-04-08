package org.apache.rocketmq.client.hook;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.Map;

@Getter
@Setter
public class ConsumeMessageContext {

    private String consumerGroup;

    private List<MessageExt> msgList;

    private MessageQueue mq;

    private boolean success;

    private String status;

    private Object mqTraceContext;

    private Map<String, String> props;

    private String namespace;
}

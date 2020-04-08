package org.apache.rocketmq.client.hook;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

@Getter
@Setter
public class FilterMessageContext {

    private String consumerGroup;

    private List<MessageExt> msgList;

    private MessageQueue mq;

    private Object arg;

    private boolean unitMode;

    @Override
    public String toString() {
        return "ConsumeMessageContext [consumerGroup=" + consumerGroup + ", msgList=" + msgList + ", mq="
                + mq + ", arg=" + arg + "]";
    }
}

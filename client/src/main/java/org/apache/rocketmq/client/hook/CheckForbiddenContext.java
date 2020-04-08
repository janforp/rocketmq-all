package org.apache.rocketmq.client.hook;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

@Getter
@Setter
public class CheckForbiddenContext {

    private String nameSrvAddr;

    private String group;

    private Message message;

    private MessageQueue mq;

    private String brokerAddr;

    private CommunicationMode communicationMode;

    private SendResult sendResult;

    private Exception exception;

    private Object arg;

    private boolean unitMode = false;

    @Override
    public String toString() {
        return "SendMessageContext [nameSrvAddr=" + nameSrvAddr + ", group=" + group + ", message=" + message
                + ", mq=" + mq + ", brokerAddr=" + brokerAddr + ", communicationMode=" + communicationMode
                + ", sendResult=" + sendResult + ", exception=" + exception + ", unitMode=" + unitMode
                + ", arg=" + arg + "]";
    }
}

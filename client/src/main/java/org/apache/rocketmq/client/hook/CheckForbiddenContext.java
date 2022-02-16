package org.apache.rocketmq.client.hook;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

@Getter
@Setter
@ToString
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
}

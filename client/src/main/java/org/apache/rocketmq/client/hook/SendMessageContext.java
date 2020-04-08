package org.apache.rocketmq.client.hook;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;

import java.util.Map;

@Getter
@Setter
public class SendMessageContext {

    private String producerGroup;

    private Message message;

    private MessageQueue mq;

    private String brokerAddr;

    private String bornHost;

    private CommunicationMode communicationMode;

    private SendResult sendResult;

    private Exception exception;

    private Object mqTraceContext;

    private Map<String, String> props;

    private DefaultMQProducerImpl producer;

    private MessageType msgType = MessageType.Normal_Msg;

    private String namespace;
}

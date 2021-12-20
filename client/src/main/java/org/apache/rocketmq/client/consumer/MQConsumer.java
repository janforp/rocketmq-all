package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Set;

/**
 * Message queue consumer interface
 */
public interface MQConsumer extends MQAdmin {

    /**
     * If consuming failure,message will be send back to the brokers,and delay consuming some time
     */
    @Deprecated
    void sendMessageBack(final MessageExt msg, final int delayLevel) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * If consuming failure,message will be send back to the broker,and delay consuming some time
     */
    void sendMessageBack(final MessageExt msg, final int delayLevel, final String brokerName) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * Fetch message queues from consumer cache according to the topic
     *
     * @param topic message topic
     * @return queue set
     */
    Set<MessageQueue> fetchSubscribeMessageQueues(final String topic) throws MQClientException;
}
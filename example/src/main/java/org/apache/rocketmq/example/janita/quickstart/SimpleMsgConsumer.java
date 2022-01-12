package org.apache.rocketmq.example.janita.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.example.SampleConstants;

/**
 * SimpleMsgConsumer
 *
 * @author zhucj
 * @since 20220120
 */
public class SimpleMsgConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("testConsumerGroup");
        consumer.setNamesrvAddr(SampleConstants.NAME_SERVER_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("order", "*");

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.println("收到了消息，条数为 " + msgs.size() + " context = " + context);
            for (MessageExt msg : msgs) {
                byte[] body = msg.getBody();
                String msgStr = new String(body);
                System.out.println("消费消息，消息内容是 " + msgStr);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
    }
}

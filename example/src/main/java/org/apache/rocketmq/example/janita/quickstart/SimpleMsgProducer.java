package org.apache.rocketmq.example.janita.quickstart;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.example.SampleConstants;
import org.apache.rocketmq.example.janita.Order;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * SimpleMsgProducer
 *
 * @author zhucj
 * @since 20220120
 */
public class SimpleMsgProducer {

    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("testProducerGroup");
        producer.setNamesrvAddr(SampleConstants.NAME_SERVER_ADDR);
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        Order order = new Order(1, "mac电脑", "23000");
        Message message = new Message("order", JSON.toJSONString(order).getBytes(StandardCharsets.UTF_8));
        try {
            SendResult result = producer.send(message);
            System.out.println("消息发送结果是 " + result);
        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
package org.apache.rocketmq.client.consumer.rebalance;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.ArrayList;
import java.util.List;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {

    private final InternalLogger log = ClientLogger.getLog();

    /**
     * Allocating by consumer id
     *
     * @param consumerGroup current consumer group 消费者
     * @param currentCID current consumer id 当前消费者id
     * @param mqAll message queue set in current topic topic下的所有队列
     * @param cidAll consumer set in current consumer group 所有消费者实例集合
     * @return The allocate result of given strategy
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID/*TODO 当前消费者id*/, List<MessageQueue> mqAll/*topic下的所有队列*/, List<String> cidAll/*所有消费者实例集合*/) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}", consumerGroup, currentCID, cidAll);

            // 如果需要获取队列的实例不在整体实例列表中，则返回一个空队列集合，表示读取消费者id没有分配到任何一个队列
            return result;
        }

        // cidAll.contains(currentCID) == true

        // 当前实例的下标
        int index = cidAll.indexOf(currentCID);
        // 平均分配之后还多出来的队列数量
        int mod = mqAll.size() % cidAll.size();

        int averageSize = mqAll.size() <= cidAll.size() ?
                1 : // 如果队列总数量还小于消费者的数量，则平均每个消费者最多能分配到一个队列
                (mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 : mqAll.size() / cidAll.size()); // 如果队列总数量大于消费者的数量

        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}

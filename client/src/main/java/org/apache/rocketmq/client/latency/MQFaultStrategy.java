package org.apache.rocketmq.client.latency;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.List;

/**
 * 如果开启，在随机递增取模的基础上，再过滤掉not available的Broker代理。所谓的"latencyFaultTolerance"，是指对之前失败的，按一定的时间做退避。
 * 例如，如果上次请求的latency超过550Lms，就退避3000Lms；超过1000L，就退避60000L；如果关闭，采用随机递增取模的方式选择一个队列（MessageQueue）来发送消息，
 * latencyFaultTolerance机制是实现消息发送高可用的核心关键所在。
 */
public class MQFaultStrategy {

    private final static InternalLogger log = ClientLogger.getLog();

    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    // 发送延迟故障是否启用
    @Getter
    @Setter
    private boolean sendLatencyFaultEnable = false;

    @Getter
    @Setter
    private long[] latencyMax = { 50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L };

    @Getter
    @Setter
    private long[] notAvailableDuration = { 0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L };

    /**
     * @param tpInfo 主题信息
     * @param lastBrokerName 上次发送失败的 brokerName
     * @return 队列
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo/*订阅信息*/, final String lastBrokerName/*上次发送失败的 broker*/) {
        if (this.sendLatencyFaultEnable) {
            // 开关打开了
            try {
                // 获取自增序号
                int index = tpInfo.getSendWhichQueue().getAndIncrement();

                // 该主题队列列表
                List<MessageQueue> messageQueueList = tpInfo.getMessageQueueList();

                /**
                 * 第一步：
                 * 获取一个在延迟上可以接受，并且和上次发送相同的 broker，首先获取一个自增序号，取模得到下标pos，如果该pos对应的broker
                 * 的延迟时间是可以接受的，并且是第一次发送，或者和上次发送的broker相同，则将queue返回
                 */
                for (int i = 0; i < messageQueueList.size(); i++) {
                    int pos = Math.abs(index++) % messageQueueList.size();
                    if (pos < 0) {
                        pos = 0;
                    }

                    // 根据下班选择一个队列
                    MessageQueue mq = messageQueueList.get(pos);

                    // 一个主题可能会存在于多个 broker 上，而每个 broker 的延迟情况是不一样的
                    String brokerName = mq.getBrokerName();

                    boolean available = latencyFaultTolerance.isAvailable(brokerName);
                    // 延迟状态是可以接受的
                    if (available) {
                        // 选择的 brokerName 是可用的状态
                        if (null == lastBrokerName/*上次失败的为空，说明本次是第一次发送*/ || brokerName.equals(lastBrokerName)/*不是第一次发送*/) {
                            return mq;
                        }
                    }
                }

                /**
                 * 第二步：
                 * 第一步没有找到队列的情况下，进入第二步。
                 */

                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            // 随机选择一个
            return tpInfo.selectOneMessageQueue();
        }

        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency/*发送消息的耗时*/, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency/*发送消息的耗时*/);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency/*发送消息的耗时*/) {
        // { 50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L };
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency/*发送消息的耗时*/ >= latencyMax[i]) {
                // { 0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L };
                return this.notAvailableDuration[i];
            }
        }
        return 0;
    }
}

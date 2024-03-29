package org.apache.rocketmq.client.impl.producer;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

import java.util.ArrayList;
import java.util.List;

@ToString
public class TopicPublishInfo {

    @Setter
    @Getter
    private boolean orderTopic = false;

    @Setter
    @Getter
    private boolean haveTopicRouterInfo = false;

    /**
     * 主题下的队列
     */
    @Setter
    @Getter
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();

    @Setter
    @Getter
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();

    // 主题路由数据
    @Setter
    @Getter
    private TopicRouteData topicRouteData;

    public boolean ok() {
        //messageQueueList 不为空即可
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    /**
     * @param lastBrokerName 上次发送失败的 brokerName,第一次发送的是为null
     * @return 队列
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName/*上次发送失败的 brokerName,第一次发送的是为null*/) {
        if (lastBrokerName == null) {
            // 第一次发送
            return selectOneMessageQueue();
        }
        int index = this.sendWhichQueue.getAndIncrement();
        for (int i = 0; i < this.messageQueueList.size(); i++) {
            int pos = Math.abs(index++) % this.messageQueueList.size();
            if (pos < 0) {
                pos = 0;
            }
            MessageQueue mq = this.messageQueueList.get(pos);
            if (!mq.getBrokerName().equals(lastBrokerName)) {
                // 跳过上次失败的 brokerName
                return mq;
            }
        }
        return selectOneMessageQueue();
    }

    /**
     * 取模即可
     */
    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.getAndIncrement();

        // 取模
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0) {
            pos = 0;
        }

        // 正常情况就是按顺序拿
        return this.messageQueueList.get(pos);
    }

    public int getQueueIdByBroker(final String brokerName) {
        // 队列列表
        List<QueueData> queueDataList = topicRouteData.getQueueDatas();
        for (final QueueData queueData : queueDataList) {
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }
}

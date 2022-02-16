package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConsumerGroupInfo {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final String groupName;

    /**
     * 订阅信息
     */
    @Getter
    private final ConcurrentMap<String/* Topic */, SubscriptionData/*订阅信息*/> subscriptionTable = new ConcurrentHashMap<>();

    /**
     * broker 与消费者服务的连接
     */
    @Getter
    private final ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = new ConcurrentHashMap<>(16);

    @Getter
    @Setter
    private volatile ConsumeType consumeType;

    @Getter
    @Setter
    private volatile MessageModel messageModel;

    @Getter
    @Setter
    private volatile ConsumeFromWhere consumeFromWhere;

    @Getter
    @Setter
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        this.groupName = groupName;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
    }

    // 根据 clientId 查询
    public ClientChannelInfo findChannel(final String clientId) {
        for (Entry<Channel, ClientChannelInfo> next : this.channelInfoTable.entrySet()) {

            ClientChannelInfo channelInfo = next.getValue();
            if (channelInfo.getClientId().equals(clientId)) {
                // 根据 clientId 查询
                return channelInfo;
            }
        }
        return null;
    }

    public List<Channel> getAllChannel() {
        return new ArrayList<>(this.channelInfoTable.keySet());
    }

    public List<String> getAllClientId() {
        List<String> result = new ArrayList<>();
        for (Entry<Channel, ClientChannelInfo> entry : this.channelInfoTable.entrySet()) {
            ClientChannelInfo clientChannelInfo = entry.getValue();
            String clientId = clientChannelInfo.getClientId();
            result.add(clientId);
        }

        return result;
    }

    public void unregisterChannel(final ClientChannelInfo clientChannelInfo) {
        Channel channel = clientChannelInfo.getChannel();
        ClientChannelInfo old = this.channelInfoTable.remove(channel);
        if (old != null) {
            log.info("unregister a consumer[{}] from consumerGroupInfo {}", this.groupName, old.toString());
        }
    }

    public boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        final ClientChannelInfo info = this.channelInfoTable.remove(channel);
        if (info != null) {
            log.warn("NETTY EVENT: remove not active channel[{}] from ConsumerGroupInfo groupChannelTable, consumer group: {}", info.toString(), groupName);
            return true;
        }
        return false;
    }

    /**
     * @see ConsumerManager#registerConsumer
     */
    // 修改 或者  添加
    public boolean updateChannel(final ClientChannelInfo infoNew, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        boolean updated = false;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
        Channel newChannel = infoNew.getChannel();
        ClientChannelInfo infoOld = this.channelInfoTable.get(newChannel);
        if (null == infoOld/*新的*/) {
            ClientChannelInfo prev = this.channelInfoTable.put(newChannel, infoNew);
            if (null == prev) {
                log.info("new consumer connected, group: {} {} {} channel: {}", this.groupName, consumeType, messageModel, infoNew.toString());
                // 真正的修改
                updated = true;
            }
            infoOld = infoNew;
        } else {
            if (!infoOld.getClientId().equals(infoNew.getClientId())/*clientId 发生了变化*/) {
                log.error("[BUG] consumer channel exist in broker, but clientId not equal. GROUP: {} OLD: {} NEW: {} ", this.groupName, infoOld.toString(), infoNew.toString());
                this.channelInfoTable.put(newChannel, infoNew);
            }
        }
        this.lastUpdateTimestamp = System.currentTimeMillis();
        infoOld.setLastUpdateTimestamp(this.lastUpdateTimestamp);
        return updated;
    }

    /**
     * @see ConsumerManager#registerConsumer
     */
    public boolean updateSubscription(final Set<SubscriptionData> subList) {
        boolean updated = false;
        for (SubscriptionData sub : subList) {
            String subTopic = sub.getTopic();
            // 从历史数据(上一次心跳发送过来的)中拿
            SubscriptionData old = this.subscriptionTable.get(subTopic);
            if (old == null/*说明该主题是新的*/) {
                SubscriptionData prev = this.subscriptionTable.putIfAbsent(subTopic, sub);
                if (null == prev) {
                    // 之前没有 subTopic 对应的数据，则为修改
                    updated = true;
                    log.info("subscription changed, add new topic, group: {} {}", this.groupName, sub.toString());
                }
            } else if (sub.getSubVersion() > old.getSubVersion()/*比较版本*/) {
                if (this.consumeType == ConsumeType.CONSUME_PASSIVELY) {
                    log.info("subscription changed, group: {} OLD: {} NEW: {}", this.groupName, old.toString(), sub.toString());
                }
                // 如果版本更新了，则替换上一次心跳的数据
                this.subscriptionTable.put(subTopic, sub);
            }
        }

        // ConcurrentMap<String/* Topic */, SubscriptionData> subscriptionTable
        Iterator<Entry<String, SubscriptionData>> it = this.subscriptionTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionData> next = it.next();
            String oldTopic = next.getKey();
            boolean exist = false;
            for (SubscriptionData sub : subList) {
                if (sub.getTopic().equals(oldTopic)) {
                    exist = true;
                    break;
                }
            }

            if (!exist) {
                // 移除已经不订阅的主题
                log.warn("subscription changed, group: {} remove topic {} {}", this.groupName, oldTopic, next.getValue().toString());
                it.remove();
                updated = true;
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();

        return updated;
    }

    public Set<String> getSubscribeTopics() {
        return subscriptionTable.keySet();
    }

    public SubscriptionData findSubscriptionData(final String topic) {
        return this.subscriptionTable.get(topic);
    }
}

package org.apache.rocketmq.broker.offset;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.MessageStore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 文件：/Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/config/consumerOffset.json
 * * {
 * *   "offsetTable": {
 * *     "TopicTest@testConsumerGroup": {
 * *       0: 250,
 * *       1: 250,
 * *       2: 250,
 * *       3: 250
 * *     },
 * *     "%RETRY%testConsumerGroup@testConsumerGroup": {
 * *       0: 0
 * *     },
 * *     "order@testConsumerGroup": {
 * *       0: 1,
 * *       1: 1,
 * *       2: 1,
 * *       3: 4
 * *     }
 * *   }
 * * }
 */
@NoArgsConstructor
public class ConsumerOffsetManager extends ConfigManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final String TOPIC_GROUP_SEPARATOR = "@";

    /**
     * 文件：/Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/config/consumerOffset.json
     * * {
     * *   "offsetTable": {
     * *     "TopicTest@testConsumerGroup": {
     * *       0: 250,
     * *       1: 250,
     * *       2: 250,
     * *       3: 250
     * *     },
     * *     "%RETRY%testConsumerGroup@testConsumerGroup": {
     * *       0: 0
     * *     },
     * *     "order@testConsumerGroup": {
     * *       0: 1,
     * *       1: 1,
     * *       2: 1,
     * *       3: 4
     * *     }
     * *   }
     * * }
     *
     * @see ConsumerOffsetManager#decode(java.lang.String)
     */
    @Getter
    @Setter
    private ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer/*queueId*/, Long/*该队列的offset*/>> offsetTable = new ConcurrentHashMap<>(512);

    private transient BrokerController brokerController;

    public ConsumerOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @SuppressWarnings("unused")
    public void scanUnsubscribedTopic() {
        Iterator<Entry<String/* topic@group */, ConcurrentMap<Integer/*queueId*/, Long/*该队列的offset*/>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String/* topic@group */, ConcurrentMap<Integer/*queueId*/, Long/*该队列的offset*/>> next = it.next();

            // order@testConsumerGroup
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                String topic = arrays[0];
                String group = arrays[1];

                // 消费者管理服务
                ConsumerManager consumerManager = brokerController.getConsumerManager();

                // 根据消费者组以及主题查询订阅信息
                SubscriptionData subscriptionData = consumerManager.findSubscriptionData(group, topic);
                ConcurrentMap<Integer/*queueId*/, Long/*该队列的offset*/> queueIdOffSet = next.getValue();
                boolean offsetBehindMuchThanData = this.offsetBehindMuchThanData(topic, queueIdOffSet);

                if (null == subscriptionData/*订阅信息不存在*/ && offsetBehindMuchThanData) {
                    it.remove();
                    log.warn("remove topic offset, {}", topicAtGroup);
                }
            }
        }
    }

    private boolean offsetBehindMuchThanData(final String topic, ConcurrentMap<Integer/*queueId*/, Long/*该队列的offset*/> table) {
        Iterator<Entry<Integer, Long>> it = table.entrySet().iterator();
        boolean result = !table.isEmpty();

        MessageStore messageStore = this.brokerController.getMessageStore();
        while (it.hasNext() && result) {
            Entry<Integer/*queueId*/, Long/*该队列的offset*/> next = it.next();
            Integer queueId = next.getKey();

            // 获取最小偏移量
            long minOffsetInStore = messageStore.getMinOffsetInQueue(topic, queueId);

            // 本地偏移量
            long offsetInPersist = next.getValue();

            // 本地的偏移量还小于文件中的
            result = offsetInPersist <= minOffsetInStore;
        }

        return result;
    }

    /**
     * 查询该 group 下面的所有 topic 集合
     *
     * @param group 消费者组
     * @return 该 group 下面的所有 topic 集合
     */
    public Set<String> whichTopicByConsumer(final String group) {
        Set<String> topics = new HashSet<>();

        for (Entry<String/* topic@group */, ConcurrentMap<Integer/*queueId*/, Long/*该队列的offset*/>> next : this.offsetTable.entrySet()) {
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                if (group.equals(arrays[1])) {
                    topics.add(arrays[0]);
                }
            }
        }

        return topics;
    }

    /**
     * 根据主题 查询 订阅该主题的 消费者组
     *
     * @param topic 主题
     * @return 订阅该主题的 消费者组
     */
    public Set<String> whichGroupByTopic(final String topic) {
        Set<String> groups = new HashSet<>();

        for (Entry<String/* topic@group */, ConcurrentMap<Integer/*queueId*/, Long/*该队列的offset*/>> next : this.offsetTable.entrySet()) {
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                if (topic.equals(arrays[0])) {
                    groups.add(arrays[1]);
                }
            }
        }

        return groups;
    }

    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId, final long offset) {
        // topic@group 组成 key
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        this.commitOffset(clientHost, key, queueId, offset);
    }

    private void commitOffset(final String clientHost, final String key /*topic@group*/, final int queueId, final long offset) {
        ConcurrentMap<Integer/*queueId*/, Long/*该队列的offset*/> map = this.offsetTable.get(key/*topic@group*/);
        if (null == map) {

            // 首次提交偏移量，则初始化
            map = new ConcurrentHashMap<>(32);
            map.put(queueId, offset);
            this.offsetTable.put(key, map);
        } else {
            // 非首次提交偏移量
            Long storeOffset = map.put(queueId, offset);
            if (storeOffset != null && offset < storeOffset) {
                log.warn("[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, queueId={}, requestOffset={}, storeOffset={}", clientHost, key, queueId, offset, storeOffset);
            }
        }
    }

    public long queryOffset(final String group, final String topic, final int queueId) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key/*topic@group*/);
        if (null != map) {
            Long offset = map.get(queueId);
            if (offset != null) {
                return offset;
            }
        }

        return -1;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        // consumerOffset.json 文件
        return BrokerPathConfigHelper.getConsumerOffsetPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);
            if (obj != null) {
                this.offsetTable = obj.offsetTable;
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public Map<Integer, Long> queryMinOffsetInAllGroup(final String topic, final String filterGroups/*用 , 隔开的组名称*/) {
        Map<Integer, Long> queueMinOffset = new HashMap<>();

        // 保留移除之后，剩余的 topic@group
        Set<String /* topic@group */> topicGroups = this.offsetTable.keySet();
        if (!UtilAll.isBlank(filterGroups)) {
            String[] groupArr = filterGroups.split(",");
            for (String group : groupArr) {
                // 移除 组名称 为  group 的key
                topicGroups.removeIf(topicAtGroup -> group.equals(topicAtGroup.split(TOPIC_GROUP_SEPARATOR)[1]));
            }
        }

        for (Entry<String/* topic@group */, ConcurrentMap<Integer/*queueId*/, Long/*该队列的offset*/>> offSetEntry : this.offsetTable.entrySet()) {

            // topic@group
            String topicGroup = offSetEntry.getKey();
            String[] topicGroupArr = topicGroup.split(TOPIC_GROUP_SEPARATOR);
            if (topic.equals(topicGroupArr[0])) {
                // 队列id的偏移量映射
                ConcurrentMap<Integer/*queueId*/, Long/*该队列的offset*/> value = offSetEntry.getValue();

                for (Entry<Integer/*queueId*/, Long/*该队列的offset*/> entry : value.entrySet()) {
                    Integer queueId = entry.getKey();
                    Long offsetInTable = entry.getValue();
                    long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
                    if (offsetInTable >= minOffset) {
                        Long offset = queueMinOffset.get(queueId);
                        if (offset == null) {
                            queueMinOffset.put(queueId, Math.min(Long.MAX_VALUE, offset));
                        } else {
                            queueMinOffset.put(queueId, Math.min(offset, offset));
                        }
                    }
                }
            }

        }
        return queueMinOffset;
    }

    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        return this.offsetTable.get(key);
    }

    public void cloneOffset(final String srcGroup, final String destGroup, final String topic) {
        ConcurrentMap<Integer, Long> offsets = this.offsetTable.get(topic + TOPIC_GROUP_SEPARATOR + srcGroup);
        if (offsets != null) {
            this.offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + destGroup, new ConcurrentHashMap<>(offsets));
        }
    }
}
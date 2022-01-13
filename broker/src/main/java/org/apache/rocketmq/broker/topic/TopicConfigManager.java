package org.apache.rocketmq.broker.topic;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 加载的是文件：/Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/config/topics.json
 *
 * * {
 * * 	"dataVersion":{
 * * 		"counter":4,
 * * 		"timestamp":1641969633060
 * *        },
 * * 	"topicConfigTable":{
 * * 		"TopicTest":{
 * * 			"order":false,
 * * 			"perm":6,
 * * 			"readQueueNums":4,
 * * 			"topicFilterType":"SINGLE_TAG",
 * * 			"topicName":"TopicTest",
 * * 			"topicSysFlag":0,
 * * 			"writeQueueNums":4
 * *        },
 * * 		"order":{
 * * 			"order":false,
 * * 			"perm":6,
 * * 			"readQueueNums":4,
 * * 			"topicFilterType":"SINGLE_TAG",
 * * 			"topicName":"order",
 * * 			"topicSysFlag":0,
 * * 			"writeQueueNums":4
 * *        },
 * * 		"SELF_TEST_TOPIC":{
 * * 			"order":false,
 * * 			"perm":6,
 * * 			"readQueueNums":1,
 * * 			"topicFilterType":"SINGLE_TAG",
 * * 			"topicName":"SELF_TEST_TOPIC",
 * * 			"topicSysFlag":0,
 * * 			"writeQueueNums":1
 * *        },
 * * 		"rocketmq-cluster_REPLY_TOPIC":{
 * * 			"order":false,
 * * 			"perm":6,
 * * 			"readQueueNums":1,
 * * 			"topicFilterType":"SINGLE_TAG",
 * * 			"topicName":"rocketmq-cluster_REPLY_TOPIC",
 * * 			"topicSysFlag":0,
 * * 			"writeQueueNums":1
 * *        },
 * * 		"rocketmq-cluster":{
 * * 			"order":false,
 * * 			"perm":7,
 * * 			"readQueueNums":16,
 * * 			"topicFilterType":"SINGLE_TAG",
 * * 			"topicName":"rocketmq-cluster",
 * * 			"topicSysFlag":0,
 * * 			"writeQueueNums":16
 * *        },
 * * 		"RMQ_SYS_TRANS_HALF_TOPIC":{
 * * 			"order":false,
 * * 			"perm":6,
 * * 			"readQueueNums":1,
 * * 			"topicFilterType":"SINGLE_TAG",
 * * 			"topicName":"RMQ_SYS_TRANS_HALF_TOPIC",
 * * 			"topicSysFlag":0,
 * * 			"writeQueueNums":1
 * *        },
 * * 		"broker-a":{
 * * 			"order":false,
 * * 			"perm":7,
 * * 			"readQueueNums":1,
 * * 			"topicFilterType":"SINGLE_TAG",
 * * 			"topicName":"broker-a",
 * * 			"topicSysFlag":0,
 * * 			"writeQueueNums":1
 * *        },
 * * 		"TBW102":{
 * * 			"order":false,
 * * 			"perm":7,
 * * 			"readQueueNums":4,
 * * 			"topicFilterType":"SINGLE_TAG",
 * * 			"topicName":"TBW102",
 * * 			"topicSysFlag":0,
 * * 			"writeQueueNums":4
 * *        },
 * * 		"BenchmarkTest":{
 * * 			"order":false,
 * * 			"perm":6,
 * * 			"readQueueNums":1024,
 * * 			"topicFilterType":"SINGLE_TAG",
 * * 			"topicName":"BenchmarkTest",
 * * 			"topicSysFlag":0,
 * * 			"writeQueueNums":1024
 * *        },
 * * 		"OFFSET_MOVED_EVENT":{
 * * 			"order":false,
 * * 			"perm":6,
 * * 			"readQueueNums":1,
 * * 			"topicFilterType":"SINGLE_TAG",
 * * 			"topicName":"OFFSET_MOVED_EVENT",
 * * 			"topicSysFlag":0,
 * * 			"writeQueueNums":1
 * *        },
 * * 		"%RETRY%testConsumerGroup":{
 * * 			"order":false,
 * * 			"perm":6,
 * * 			"readQueueNums":1,
 * * 			"topicFilterType":"SINGLE_TAG",
 * * 			"topicName":"%RETRY%testConsumerGroup",
 * * 			"topicSysFlag":0,
 * * 			"writeQueueNums":1
 * *        }
 * *    }
 * * }
 */
@NoArgsConstructor
public class TopicConfigManager extends ConfigManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final long LOCK_TIMEOUT_MILLIS = 3000; // 锁超时时间

    // 锁
    private transient final Lock lockTopicConfigTable = new ReentrantLock();

    //key是topicName，value是主题对应的配置
    @Getter
    private final ConcurrentMap<String/*主题，如：order*/, TopicConfig/*主题配置*/> topicConfigTable = new ConcurrentHashMap<>(1024);

    //配置的版本
    @Getter
    private final DataVersion dataVersion = new DataVersion();

    /**
     * 系统配置的topic
     * 系统自动生成的主题集合，包括：
     * SELF_TEST_TOPIC
     */
    private final Set<String> systemTopicList = new HashSet<>();

    private transient BrokerController brokerController;

    public TopicConfigManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        {
            // MixAll.SELF_TEST_TOPIC
            String topic = MixAll.SELF_TEST_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        BrokerConfig brokerConfig = this.brokerController.getBrokerConfig();
        {
            // MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC
            boolean autoCreateTopicEnable = brokerConfig.isAutoCreateTopicEnable();

            if (autoCreateTopicEnable) {
                String topic = MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC;
                TopicConfig topicConfig = new TopicConfig(topic);
                this.systemTopicList.add(topic);
                topicConfig.setReadQueueNums(brokerConfig.getDefaultTopicQueueNums()/*8*/);
                topicConfig.setWriteQueueNums(brokerConfig.getDefaultTopicQueueNums());
                int perm = PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE;
                topicConfig.setPerm(perm);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
        {
            // MixAll.BENCHMARK_TOPIC
            String topic = MixAll.BENCHMARK_TOPIC;
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1024);
            topicConfig.setWriteQueueNums(1024);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {

            String topic = brokerConfig.getBrokerClusterName();
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            int perm = PermName.PERM_INHERIT;
            if (brokerConfig.isClusterTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {

            String topic = brokerConfig.getBrokerName();
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            int perm = PermName.PERM_INHERIT;
            if (brokerConfig.isBrokerTopicEnable()) {
                perm |= PermName.PERM_READ | PermName.PERM_WRITE;
            }
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            topicConfig.setPerm(perm);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            // MixAll.OFFSET_MOVED_EVENT
            String topic = MixAll.OFFSET_MOVED_EVENT;
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        {
            if (brokerConfig.isTraceTopicEnable()) {
                String topic = brokerConfig.getMsgTraceTopicName();
                TopicConfig topicConfig = new TopicConfig(topic);
                this.systemTopicList.add(topic);
                topicConfig.setReadQueueNums(1);
                topicConfig.setWriteQueueNums(1);
                this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
            }
        }
        {
            String topic = brokerConfig.getBrokerClusterName() + "_" + MixAll.REPLY_TOPIC_POSTFIX /*REPLY_TOPIC*/;
            TopicConfig topicConfig = new TopicConfig(topic);
            this.systemTopicList.add(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
    }

    public boolean isSystemTopic(final String topic) {
        return this.systemTopicList.contains(topic);
    }

    public Set<String> getSystemTopic() {
        return this.systemTopicList;
    }

    public TopicConfig selectTopicConfig(final String topic) {
        return this.topicConfigTable.get(topic);
    }

    /**
     * 在发送消息的方法中，创建topic,返回对应的TopicConfig,步骤如下:
     * 1.topicConfigTable有记录直接返回
     * 2.defaultTopicConfig有记录，且有继承权限的话，就创建
     *
     * @param defaultTopic: 默认topic，参照SendMessageRequestHeader#setDefaultTopic调用方，默认"TBW102"
     * @param clientDefaultTopicQueueNums： 创建时，默认的topic队列数量，参照SendMessageRequestHeader#setDefaultTopicQueueNums调用方，默认4
     */
    public TopicConfig createTopicInSendMessageMethod(final String topic, final String defaultTopic, final String remoteAddress, final int clientDefaultTopicQueueNums, final int topicSysFlag) {
        TopicConfig topicConfig = null;
        boolean createNew = false;

        try {
            if (this.lockTopicConfigTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    // ConcurrentMap<String/*主题，如：order*/, TopicConfig/*主题配置*/> topicConfigTable
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null) {
                        return topicConfig;
                    }

                    // ConcurrentMap<String/*主题，如：order*/, TopicConfig/*主题配置*/> topicConfigTable
                    TopicConfig defaultTopicConfig/*主题配置*/ = this.topicConfigTable.get(defaultTopic);
                    if (defaultTopicConfig != null) {
                        if (defaultTopic.equals(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC /*TBW102*/)) {
                            if (!this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                                defaultTopicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
                            }
                        }

                        if (PermName.isInherited(defaultTopicConfig.getPerm())) {
                            topicConfig = new TopicConfig(topic);
                            int queueNums = clientDefaultTopicQueueNums > defaultTopicConfig.getWriteQueueNums() ? defaultTopicConfig.getWriteQueueNums() : clientDefaultTopicQueueNums;
                            if (queueNums < 0) {
                                queueNums = 0;
                            }
                            topicConfig.setReadQueueNums(queueNums);
                            topicConfig.setWriteQueueNums(queueNums);
                            int perm = defaultTopicConfig.getPerm();
                            perm &= ~PermName.PERM_INHERIT;
                            topicConfig.setPerm(perm);
                            topicConfig.setTopicSysFlag(topicSysFlag);
                            topicConfig.setTopicFilterType(defaultTopicConfig.getTopicFilterType());
                        } else {
                            log.warn("Create new topic failed, because the default topic[{}] has no perm [{}] producer:[{}]", defaultTopic, defaultTopicConfig.getPerm(), remoteAddress);
                        }
                    } else {
                        log.warn("Create new topic failed, because the default topic[{}] not exist. producer:[{}]", defaultTopic, remoteAddress);
                    }

                    if (topicConfig != null) {
                        log.info("Create new topic by default topic:[{}] config:[{}] producer:[{}]", defaultTopic, topicConfig, remoteAddress);
                        this.topicConfigTable.put(topic, topicConfig);
                        this.dataVersion.nextVersion();
                        createNew = true;
                        this.persist();
                    }
                } finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageMethod exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll(false, true, true);
        }

        return topicConfig;
    }

    public TopicConfig createTopicInSendMessageBackMethod(final String topic, final int clientDefaultTopicQueueNums, final int perm, final int topicSysFlag) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            return topicConfig;
        }

        boolean createNew = false;

        try {
            if (this.lockTopicConfigTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(topic);
                    if (topicConfig != null) {
                        return topicConfig;
                    }

                    topicConfig = new TopicConfig(topic);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(topicSysFlag);

                    log.info("create new topic {}", topicConfig);
                    this.topicConfigTable.put(topic, topicConfig);
                    createNew = true;
                    this.dataVersion.nextVersion();
                    this.persist();
                } finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("createTopicInSendMessageBackMethod exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll(false, true, true);
        }

        return topicConfig;
    }

    public TopicConfig createTopicOfTranCheckMaxTime(final int clientDefaultTopicQueueNums, final int perm) {
        TopicConfig topicConfig = this.topicConfigTable.get(MixAll.TRANS_CHECK_MAX_TIME_TOPIC);
        if (topicConfig != null) {
            return topicConfig;
        }

        boolean createNew = false;

        try {
            if (this.lockTopicConfigTable.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    topicConfig = this.topicConfigTable.get(MixAll.TRANS_CHECK_MAX_TIME_TOPIC);
                    if (topicConfig != null) {
                        return topicConfig;
                    }

                    topicConfig = new TopicConfig(MixAll.TRANS_CHECK_MAX_TIME_TOPIC);
                    topicConfig.setReadQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setWriteQueueNums(clientDefaultTopicQueueNums);
                    topicConfig.setPerm(perm);
                    topicConfig.setTopicSysFlag(0);

                    log.info("create new topic {}", topicConfig);
                    this.topicConfigTable.put(MixAll.TRANS_CHECK_MAX_TIME_TOPIC, topicConfig);
                    createNew = true;
                    this.dataVersion.nextVersion();
                    this.persist();
                } finally {
                    this.lockTopicConfigTable.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("create TRANS_CHECK_MAX_TIME_TOPIC exception", e);
        }

        if (createNew) {
            this.brokerController.registerBrokerAll(false, true, true);
        }

        return topicConfig;
    }

    public void updateTopicUnitFlag(final String topic, final boolean unit) {

        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (unit) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitFlag(oldTopicSysFlag));
            } else {
                topicConfig.setTopicSysFlag(TopicSysFlag.clearUnitFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                    topicConfig.getTopicSysFlag());

            this.topicConfigTable.put(topic, topicConfig);

            this.dataVersion.nextVersion();

            this.persist();
            this.brokerController.registerBrokerAll(false, true, true);
        }
    }

    public void updateTopicUnitSubFlag(final String topic, final boolean hasUnitSub) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig != null) {
            int oldTopicSysFlag = topicConfig.getTopicSysFlag();
            if (hasUnitSub) {
                topicConfig.setTopicSysFlag(TopicSysFlag.setUnitSubFlag(oldTopicSysFlag));
            }

            log.info("update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag", oldTopicSysFlag,
                    topicConfig.getTopicSysFlag());

            this.topicConfigTable.put(topic, topicConfig);

            this.dataVersion.nextVersion();

            this.persist();
            this.brokerController.registerBrokerAll(false, true, true);
        }
    }

    public void updateTopicConfig(final TopicConfig topicConfig) {
        TopicConfig old = this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        if (old != null) {
            log.info("update topic config, old:[{}] new:[{}]", old, topicConfig);
        } else {
            log.info("create new topic [{}]", topicConfig);
        }

        this.dataVersion.nextVersion();

        this.persist();
    }

    public void updateOrderTopicConfig(final KVTable orderKVTableFromNs) {

        if (orderKVTableFromNs != null && orderKVTableFromNs.getTable() != null) {
            boolean isChange = false;
            Set<String> orderTopics = orderKVTableFromNs.getTable().keySet();
            for (String topic : orderTopics) {
                TopicConfig topicConfig = this.topicConfigTable.get(topic);
                if (topicConfig != null && !topicConfig.isOrder()) {
                    topicConfig.setOrder(true);
                    isChange = true;
                    log.info("update order topic config, topic={}, order={}", topic, true);
                }
            }

            for (Map.Entry<String, TopicConfig> entry : this.topicConfigTable.entrySet()) {
                String topic = entry.getKey();
                if (!orderTopics.contains(topic)) {
                    TopicConfig topicConfig = entry.getValue();
                    if (topicConfig.isOrder()) {
                        topicConfig.setOrder(false);
                        isChange = true;
                        log.info("update order topic config, topic={}, order={}", topic, false);
                    }
                }
            }

            if (isChange) {
                this.dataVersion.nextVersion();
                this.persist();
            }
        }
    }

    public boolean isOrderTopic(final String topic) {
        TopicConfig topicConfig = this.topicConfigTable.get(topic);
        if (topicConfig == null) {
            return false;
        } else {
            return topicConfig.isOrder();
        }
    }

    public void deleteTopicConfig(final String topic) {
        TopicConfig old = this.topicConfigTable.remove(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: {}", old);
            this.dataVersion.nextVersion();
            this.persist();
        } else {
            log.warn("delete topic config failed, topic: {} not exists", topic);
        }
    }

    public TopicConfigSerializeWrapper buildTopicConfigSerializeWrapper() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(this.topicConfigTable);
        topicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return topicConfigSerializeWrapper;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        MessageStoreConfig messageStoreConfig = this.brokerController.getMessageStoreConfig();
        // ../store
        String storePathRootDir = messageStoreConfig.getStorePathRootDir();
        // rootDir + File.separator + "config" + File.separator + "topics.json";

        // /Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/config/topics.json
        return BrokerPathConfigHelper.getTopicConfigPath(storePathRootDir);
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TopicConfigSerializeWrapper topicConfigSerializeWrapper =
                    TopicConfigSerializeWrapper.fromJson(jsonString, TopicConfigSerializeWrapper.class);
            if (topicConfigSerializeWrapper != null) {
                this.topicConfigTable.putAll(topicConfigSerializeWrapper.getTopicConfigTable());
                this.dataVersion.assignNewOne(topicConfigSerializeWrapper.getDataVersion());
                this.printLoadDataWhenFirstBoot(topicConfigSerializeWrapper);
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        TopicConfigSerializeWrapper serializeWrapper = new TopicConfigSerializeWrapper();
        // ConcurrentMap<String/*主题，如：order*/, TopicConfig/*主题配置*/> topicConfigTable
        serializeWrapper.setTopicConfigTable(this.topicConfigTable);
        serializeWrapper.setDataVersion(this.dataVersion);
        // json 序列化
        return serializeWrapper.toJson(prettyFormat);
    }

    private void printLoadDataWhenFirstBoot(final TopicConfigSerializeWrapper tcs) {
        for (Entry<String, TopicConfig> next : tcs.getTopicConfigTable().entrySet()) {
            log.info("load exist local topic, {}", next.getValue().toString());
        }
    }
}

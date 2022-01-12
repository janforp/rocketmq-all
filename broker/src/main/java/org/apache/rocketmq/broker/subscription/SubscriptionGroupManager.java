package org.apache.rocketmq.broker.subscription;

import lombok.Getter;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 订阅组管理
 *
 * 管理文件：/Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/config/subscriptionGroup.json
 * * {
 * * 	"dataVersion":{
 * * 		"counter":1,
 * * 		"timestamp":1641967891916
 * *        },
 * * 	"subscriptionGroupTable":{
 * * 		"SELF_TEST_C_GROUP":{
 * * 			"brokerId":0,
 * * 			"consumeBroadcastEnable":true,
 * * 			"consumeEnable":true,
 * * 			"consumeFromMinEnable":true,
 * * 			"groupName":"SELF_TEST_C_GROUP",
 * * 			"notifyConsumerIdsChangedEnable":true,
 * * 			"retryMaxTimes":16,
 * * 			"retryQueueNums":1,
 * * 			"whichBrokerWhenConsumeSlowly":1
 * *        },
 * * 		"CID_ONSAPI_OWNER":{
 * * 			"brokerId":0,
 * * 			"consumeBroadcastEnable":true,
 * * 			"consumeEnable":true,
 * * 			"consumeFromMinEnable":true,
 * * 			"groupName":"CID_ONSAPI_OWNER",
 * * 			"notifyConsumerIdsChangedEnable":true,
 * * 			"retryMaxTimes":16,
 * * 			"retryQueueNums":1,
 * * 			"whichBrokerWhenConsumeSlowly":1
 * *        },
 * * 		"testConsumerGroup":{
 * * 			"brokerId":0,
 * * 			"consumeBroadcastEnable":true,
 * * 			"consumeEnable":true,
 * * 			"consumeFromMinEnable":true,
 * * 			"groupName":"testConsumerGroup",
 * * 			"notifyConsumerIdsChangedEnable":true,
 * * 			"retryMaxTimes":16,
 * * 			"retryQueueNums":1,
 * * 			"whichBrokerWhenConsumeSlowly":1
 * *        },
 * * 		"CID_ONSAPI_PERMISSION":{
 * * 			"brokerId":0,
 * * 			"consumeBroadcastEnable":true,
 * * 			"consumeEnable":true,
 * * 			"consumeFromMinEnable":true,
 * * 			"groupName":"CID_ONSAPI_PERMISSION",
 * * 			"notifyConsumerIdsChangedEnable":true,
 * * 			"retryMaxTimes":16,
 * * 			"retryQueueNums":1,
 * * 			"whichBrokerWhenConsumeSlowly":1
 * *        },
 * * 		"TOOLS_CONSUMER":{
 * * 			"brokerId":0,
 * * 			"consumeBroadcastEnable":true,
 * * 			"consumeEnable":true,
 * * 			"consumeFromMinEnable":true,
 * * 			"groupName":"TOOLS_CONSUMER",
 * * 			"notifyConsumerIdsChangedEnable":true,
 * * 			"retryMaxTimes":16,
 * * 			"retryQueueNums":1,
 * * 			"whichBrokerWhenConsumeSlowly":1
 * *        },
 * * 		"CID_ONS-HTTP-PROXY":{
 * * 			"brokerId":0,
 * * 			"consumeBroadcastEnable":true,
 * * 			"consumeEnable":true,
 * * 			"consumeFromMinEnable":true,
 * * 			"groupName":"CID_ONS-HTTP-PROXY",
 * * 			"notifyConsumerIdsChangedEnable":true,
 * * 			"retryMaxTimes":16,
 * * 			"retryQueueNums":1,
 * * 			"whichBrokerWhenConsumeSlowly":1
 * *        },
 * * 		"FILTERSRV_CONSUMER":{
 * * 			"brokerId":0,
 * * 			"consumeBroadcastEnable":true,
 * * 			"consumeEnable":true,
 * * 			"consumeFromMinEnable":true,
 * * 			"groupName":"FILTERSRV_CONSUMER",
 * * 			"notifyConsumerIdsChangedEnable":true,
 * * 			"retryMaxTimes":16,
 * * 			"retryQueueNums":1,
 * * 			"whichBrokerWhenConsumeSlowly":1
 * *        },
 * * 		"CID_ONSAPI_PULL":{
 * * 			"brokerId":0,
 * * 			"consumeBroadcastEnable":true,
 * * 			"consumeEnable":true,
 * * 			"consumeFromMinEnable":true,
 * * 			"groupName":"CID_ONSAPI_PULL",
 * * 			"notifyConsumerIdsChangedEnable":true,
 * * 			"retryMaxTimes":16,
 * * 			"retryQueueNums":1,
 * * 			"whichBrokerWhenConsumeSlowly":1
 * *        }
 * *    }
 * * }
 */
public class SubscriptionGroupManager extends ConfigManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    @Getter
    private final ConcurrentMap<String/*TODO group?*/, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap<>(1024);

    @Getter
    private final DataVersion dataVersion = new DataVersion();

    private transient BrokerController brokerController;

    @SuppressWarnings("unused")
    public SubscriptionGroupManager() {
        this.init();
    }

    public SubscriptionGroupManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.init();
    }

    // 一些系统自待的组
    private void init() {
        {
            // TOOLS_CONSUMER
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.TOOLS_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.TOOLS_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            // FILTERSRV_CONSUMER
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.FILTERSRV_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.FILTERSRV_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.SELF_TEST_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.SELF_TEST_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.ONS_HTTP_PROXY_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.ONS_HTTP_PROXY_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PULL_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_PULL_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PERMISSION_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_PERMISSION_GROUP, subscriptionGroupConfig);
        }

        {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_OWNER_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_OWNER_GROUP, subscriptionGroupConfig);
        }
    }

    public void updateSubscriptionGroupConfig(final SubscriptionGroupConfig config) {
        // 塞入新的，返回老的
        String groupName = config.getGroupName();
        SubscriptionGroupConfig old = this.subscriptionGroupTable.put(groupName, config);
        if (old != null) {
            log.info("update subscription group config, old: {} new: {}", old, config);
        } else {
            log.info("create new subscription group, {}", config);
        }

        this.dataVersion.nextVersion();
        // 持久化到 /Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/config/subscriptionGroup.json
        this.persist();
    }

    public void disableConsume(final String groupName) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.get(groupName);
        if (old != null) {
            old.setConsumeEnable(false);
            this.dataVersion.nextVersion();
        }
    }

    public SubscriptionGroupConfig findSubscriptionGroupConfig(final String group) {
        SubscriptionGroupConfig subscriptionGroupConfig = this.subscriptionGroupTable.get(group);
        if (null == subscriptionGroupConfig) {
            if (brokerController.getBrokerConfig().isAutoCreateSubscriptionGroup() || MixAll.isSysConsumerGroup(group)) {
                subscriptionGroupConfig = new SubscriptionGroupConfig();
                subscriptionGroupConfig.setGroupName(group);
                SubscriptionGroupConfig preConfig = this.subscriptionGroupTable.putIfAbsent(group, subscriptionGroupConfig);
                if (null == preConfig) {
                    log.info("auto create a subscription group, {}", subscriptionGroupConfig.toString());
                }
                this.dataVersion.nextVersion();
                this.persist();
            }
        }

        return subscriptionGroupConfig;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        // System.getProperty("user.home") + File.separator + "store";
        String storePathRootDir = this.brokerController.getMessageStoreConfig().getStorePathRootDir();
        return BrokerPathConfigHelper.getSubscriptionGroupPath(storePathRootDir);
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            SubscriptionGroupManager obj = RemotingSerializable.fromJson(jsonString, SubscriptionGroupManager.class);
            if (obj != null) {
                this.subscriptionGroupTable.putAll(obj.subscriptionGroupTable);
                this.dataVersion.assignNewOne(obj.dataVersion);
                this.printLoadDataWhenFirstBoot(obj);
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    private void printLoadDataWhenFirstBoot(final SubscriptionGroupManager sgm) {
        for (Entry<String, SubscriptionGroupConfig> next : sgm.getSubscriptionGroupTable().entrySet()) {
            log.info("load exist subscription group, {}", next.getValue().toString());
        }
    }

    public void deleteSubscriptionGroupConfig(final String groupName) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.remove(groupName);
        if (old != null) {
            log.info("delete subscription group OK, subscription group:{}", old);
            this.dataVersion.nextVersion();
            this.persist();
        } else {
            log.warn("delete subscription group failed, subscription groupName: {} not exist", groupName);
        }
    }
}

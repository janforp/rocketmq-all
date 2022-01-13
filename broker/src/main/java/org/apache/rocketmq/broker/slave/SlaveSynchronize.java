package org.apache.rocketmq.broker.slave;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

public class SlaveSynchronize {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    @Setter
    @Getter
    private volatile String masterAddr = null;

    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void syncAll() {
        this.syncTopicConfig();
        this.syncConsumerOffset();
        this.syncDelayOffset();
        this.syncSubscriptionGroupConfig();
    }

    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;
        String brokerAddr = brokerController.getBrokerAddr();
        if (masterAddrBak != null && !masterAddrBak.equals(brokerAddr) /*当前 broker 不是 master*/) {
            try {
                BrokerOuterAPI brokerOuterAPI = this.brokerController.getBrokerOuterAPI();

                // 从 master 上拿到所有的
                TopicConfigSerializeWrapper topicWrapper = brokerOuterAPI.getAllTopicConfig(masterAddrBak);

                TopicConfigManager topicConfigManager = this.brokerController.getTopicConfigManager();

                // 跟 slave 本地比较更新
                if (!topicConfigManager.getDataVersion().equals(topicWrapper.getDataVersion())) {
                    topicConfigManager.getDataVersion().assignNewOne(topicWrapper.getDataVersion());
                    topicConfigManager.getTopicConfigTable().clear();
                    topicConfigManager.getTopicConfigTable().putAll(topicWrapper.getTopicConfigTable());
                    topicConfigManager.persist();

                    log.info("Update slave topic config from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncTopicConfig Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())/*当前 broker 不是 master*/) {
            try {
                BrokerOuterAPI brokerOuterAPI = this.brokerController.getBrokerOuterAPI();
                ConsumerOffsetSerializeWrapper offsetWrapper = brokerOuterAPI.getAllConsumerOffset(masterAddrBak);
                /*
                 * 管理消费者本地的偏移量
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
                ConsumerOffsetManager consumerOffsetManager = this.brokerController.getConsumerOffsetManager();
                ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer/*queueId*/, Long/*该队列的offset*/>> offsetTable = consumerOffsetManager.getOffsetTable();

                // 把 master 上的数据设置到集合中
                offsetTable.putAll(offsetWrapper.getOffsetTable());

                // 持久化到 slave
                consumerOffsetManager.persist();
                log.info("Update slave consumer offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    // 同步延迟队列相关的数据到本地
    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())/*当前 broker 不是 master*/) {
            try {

                // master 的数据
                String delayOffset = this.brokerController.getBrokerOuterAPI().getAllDelayOffset(masterAddrBak);
                if (delayOffset != null) {
                    // delayOffset.json 文件全路径
                    String fileName = StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
                    try {

                        // 保存到本地文件中
                        MixAll.string2File(delayOffset, fileName);
                    } catch (IOException e) {
                        log.error("Persist file Exception, {}", fileName, e);
                    }
                }
                log.info("Update slave delay offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncDelayOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    // 同步订阅信息
    private void syncSubscriptionGroupConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())/*当前 broker 不是 master*/) {
            try {

                // 从 master 上获取的订阅数据
                SubscriptionGroupWrapper subscriptionWrapper = this.brokerController.getBrokerOuterAPI().getAllSubscriptionGroupConfig(masterAddrBak);

                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion().equals(subscriptionWrapper.getDataVersion())/*只有 slave 跟 master 上的数据不一致才真正的去持久化*/) {
                    /*
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
                    SubscriptionGroupManager subscriptionGroupManager = this.brokerController.getSubscriptionGroupManager();
                    // 设置本地 slave 的版本数据跟 master 一样
                    subscriptionGroupManager.getDataVersion().assignNewOne(subscriptionWrapper.getDataVersion());
                    subscriptionGroupManager.getSubscriptionGroupTable().clear();
                    subscriptionGroupManager.getSubscriptionGroupTable().putAll(subscriptionWrapper.getSubscriptionGroupTable());

                    // 持久化
                    subscriptionGroupManager.persist();
                    log.info("Update slave Subscription Group from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
            }
        }
    }
}

package org.apache.rocketmq.client.impl.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class PullAPIWrapper {

    private final InternalLogger log = ClientLogger.getLog();

    private final MQClientInstance mQClientFactory;

    private final String consumerGroup;

    private final boolean unitMode;

    /**
     * 保存下次拉取消息的 推荐 brokerId
     */
    private final ConcurrentMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable = new ConcurrentHashMap<MessageQueue, AtomicLong>(32);

    @Setter
    @Getter
    private volatile boolean connectBrokerByUser = false;

    @Getter
    @Setter
    private volatile long defaultBrokerId = MixAll.MASTER_ID;

    private final Random random = new Random(System.currentTimeMillis());

    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    /**
     * 主要是反序列化消息以及根基消息tag过滤
     *
     * @param mq 队列
     * @param pullResult 队列拉取到的消息对象
     * @param subscriptionData 主题的订阅消息
     * @return 拉取消息处理之后的结果
     */
    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult, final SubscriptionData subscriptionData) {
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        // 关更新下次拉数据的服务器 brokerId
        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());

        // 下面是将二进制消息解码为x
        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            // 消息的二进制数组
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());

            // 反序列化成消息
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

            // 根据 tag 过滤之后的消息列表
            List<MessageExt> msgListFilterAgain = msgList;
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            // 根据 tag 过滤消息
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            for (MessageExt msg : msgListFilterAgain) {
                // TRAN_MSG
                String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(traFlag)) {
                    // 如果是事务消息，则设置事务id 为 UNIQ_KEY
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                }
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET, Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET, Long.toString(pullResult.getMaxOffset()));
                msg.setBrokerName(mq.getBrokerName());
            }

            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        // 移除二进制，因为上面已经把二进制反序列化成消息对象拉
        pullResultExt.setMessageBinary(null);

        return pullResult;
    }

    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    public PullResult pullKernelImpl(
            final MessageQueue mq, // 队列
            final String subExpression,// 订阅信息
            final String expressionType,// 一般是 TAG
            final long subVersion, // 版本
            final long offset,// 下次拉取消息的偏移量
            final int maxNums, // 批量大小
            final int sysFlag, // 如果全都是 true，则 flag 为：0    0   0   0   1   1   1   1
            final long commitOffset, // 是否提交偏移量
            final long brokerSuspendMaxTimeMillis,// 时间
            final long timeoutMillis,// 时间
            final CommunicationMode communicationMode,// 异步
            final PullCallback pullCallback// 消息拉回来之后的回调
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        // 查询 broker
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), this.recalculatePullFromWhichNode(mq) /* 计算得到 brokerId */, false);
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            // 查询 broker
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), this.recalculatePullFromWhichNode(mq)/* 计算得到 brokerId */, false);
        }

        if (findBrokerResult == null) {
            // 找不到合适的 broker，发个鸡巴

            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }

        {
            // check version
            if (!ExpressionType.isTagType(expressionType) && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                throw new MQClientException("The broker[" + mq.getBrokerName() + ", " + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
            }
        }

        // // 如果全都是 true，则 flag 为：0    0   0   0   1   1   1   1
        int sysFlagInner = sysFlag;

        if (findBrokerResult.isSlave()) {
            // 如果找的 broker 是从 broker
            // 去掉提交偏移量这个标记
            sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
        }

        PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
        requestHeader.setConsumerGroup(this.consumerGroup);
        requestHeader.setTopic(mq.getTopic());
        requestHeader.setQueueId(mq.getQueueId());
        requestHeader.setQueueOffset(offset);
        requestHeader.setMaxMsgNums(maxNums);
        requestHeader.setSysFlag(sysFlagInner);
        requestHeader.setCommitOffset(commitOffset);
        requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
        requestHeader.setSubscription(subExpression);
        requestHeader.setSubVersion(subVersion);
        requestHeader.setExpressionType(expressionType);

        String brokerAddr = findBrokerResult.getBrokerAddr();
        if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
            // 如果有类过滤器
            brokerAddr = computePullFromWhichFilterServer(mq.getTopic(), brokerAddr);
        }

        return this.mQClientFactory.getMQClientAPIImpl().pullMessage(brokerAddr, requestHeader, timeoutMillis, communicationMode, pullCallback);
    }

    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        return MixAll.MASTER_ID;
    }

    private String computePullFromWhichFilterServer(final String topic, final String brokerAddr) throws MQClientException {
        // ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);
            if (list != null && !list.isEmpty()) {
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: " + topic, null);
    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0) {
                value = 0;
            }
        }
        return value;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }
}

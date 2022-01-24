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
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class PullAPIWrapper {

    private final InternalLogger log = ClientLogger.getLog();

    private final MQClientInstance mQClientFactory;

    private final String consumerGroup;

    private final boolean unitMode;

    /**
     * 保存下次拉取消息的 推荐
     * value:推荐拉消息使用的主机id
     *
     * @see PullAPIWrapper#processPullResult(org.apache.rocketmq.common.message.MessageQueue, org.apache.rocketmq.client.consumer.PullResult, org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData) 拉取结果会返回
     */
    private final ConcurrentMap<MessageQueue/*队列*/, AtomicLong/* 下次拉消息时候，建议去该brokerId去拉消息 */> pullFromWhichNodeTable = new ConcurrentHashMap<MessageQueue, AtomicLong>(32);

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
     * 主要是反序列化消息以及根据消息tag过滤
     *
     * @param mq 队列
     * @param pullResult 队列拉取到的消息对象
     * @param subscriptionData 主题的订阅消息
     * @return 拉取消息处理之后的结果
     */
    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult/*本次拉消息的结果*/, final SubscriptionData subscriptionData) {

        // 向下转型
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        // 关更新下次拉数据的服务器 brokerId
        long suggestWhichBrokerId = pullResultExt.getSuggestWhichBrokerId();
        this.updatePullFromWhichNode(mq, suggestWhichBrokerId);

        // 下面是将二进制消息解码为对象
        PullStatus pullStatus = pullResult.getPullStatus();
        if (PullStatus.FOUND == pullStatus) {
            // 消息的二进制数组
            byte[] messageBinary = pullResultExt.getMessageBinary();
            ByteBuffer byteBuffer = ByteBuffer.wrap(messageBinary/*消息的二进制数组*/);

            // 反序列化成消息，解码
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

            // 客户端根据 tag 过滤之后的消息列表
            List<MessageExt> msgListFilterAgain = msgList;

            // 开始进行客户端tag值过滤
            Set<String> tagsSet = subscriptionData.getTagsSet();
            if (!tagsSet.isEmpty() && !subscriptionData.isClassFilterMode()) {
                // 客户端按照 tag 过滤
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                for (MessageExt msg : msgList) {
                    String msgTags = msg.getTags();
                    if (msgTags != null) {
                        if (tagsSet.contains(msgTags)) {
                            // 根据 tag 过滤消息
                            msgListFilterAgain.add(msg);
                        }

                        // TODO 那么这些被拉取到的消息，tag不满足的消息怎么办呢？？？？直接被丢吗？
                    }
                }
            }

            // 客户端执行 hook 过滤
            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            for (MessageExt msg : msgListFilterAgain) {

                // 给消息加一些 property

                // TRAN_MSG
                String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED /*TRAN_MSG*/);
                if (Boolean.parseBoolean(traFlag)) {
                    // 如果是事务消息，则设置事务id 为 UNIQ_KEY
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX /*UNIQ_KEY*/));
                }

                // 队列最小 offset
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET /*MIN_OFFSET*/, Long.toString(pullResult.getMinOffset()));
                // 队列最大 offset
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET /*MAX_OFFSET*/, Long.toString(pullResult.getMaxOffset()));
                // 消息归属broker
                msg.setBrokerName(mq.getBrokerName());
            }

            // 把过滤后的消息设置回去
            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        // 移除二进制，因为上面已经把二进制反序列化成消息对象拉，help GC
        pullResultExt.setMessageBinary(null);

        return pullResult;
    }

    private void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        // ConcurrentMap<MessageQueue/*队列*/, AtomicLong/* 下次拉消息时候，建议去该brokerId去拉消息 */> pullFromWhichNodeTable
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    private boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    private void executeHook(final FilterMessageContext context) {
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
            final int sysFlag, // 0    0   0   0   1(是否为类过滤，，默认0，一般是TAG过滤)   1(拉消息请求是否包含消费者本地该主题的订阅信息，默认0，因为心跳的时候做了这个事情)   1(是否允许服务器长轮询，默认1)   1(是否提交消费者本地的进度,如果为1则表示提交，默认1)
            final long commitOffset, // 是否提交偏移量
            final long brokerSuspendMaxTimeMillis,// 时间，15秒
            final long timeoutMillis,// 时间
            final CommunicationMode communicationMode,// 异步
            final PullCallback pullCallback// 消息拉回来之后的回调
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        // 决定从哪个 broker 拉消息
        long brokerId /* 计算得到 brokerId */ = this.recalculatePullFromWhichNode(mq);
        // 查询 broker
        String brokerName = mq.getBrokerName();

        // 从客户端本地缓存查询
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, brokerId /* 计算得到 brokerId */, false);
        if (null == findBrokerResult) {
            // 如果本地是空，则取远程拿

            String topic = mq.getTopic();
            // 从 namesrv 服务器上拿该主题的路由信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            brokerId /* 计算得到 brokerId */ = this.recalculatePullFromWhichNode(mq);
            // 查询 broker
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, brokerId/* 计算得到 brokerId */, false /*必须是该broker？*/);
        }

        if (findBrokerResult == null) {
            // 找不到合适的 broker，发个鸡巴
            throw new MQClientException("The broker[" + brokerName + "] not exist", null);
        }

        {
            // check version
            if (!ExpressionType.isTagType(expressionType) && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                throw new MQClientException("The broker[" + brokerName + ", " + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
            }
        }

        // // 如果全都是 true，则 flag 为：0    0   0   0   1   1   1   1
        int sysFlagInner = sysFlag;

        if (findBrokerResult.isSlave() /* 从节点 不需要提交 消费进度*/) {
            // 如果找的 broker 是从 broker
            // 去掉提交偏移量这个标记，因为从节点不存储这个offset东西，将 flag 标记位中这个位设置为0，表示不提交
            sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
        }

        // 创建请求消息对象，包装 pull 请求的业务参数
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

        // 主机地址
        String brokerAddr = findBrokerResult.getBrokerAddr();
        if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
            // 如果是类过滤
            // 如果有类过滤器，则找到一个支持类过滤的服务器地址
            brokerAddr = computePullFromWhichFilterServer(mq.getTopic(), brokerAddr);
        }
        MQClientAPIImpl mqClientAPIImpl = this.mQClientFactory.getMQClientAPIImpl();
        return mqClientAPIImpl.pullMessage(brokerAddr,/*本次拉消息请求的broker服务器地址*/ requestHeader/*拉消息业务参数的封装对象*/, timeoutMillis/*30s的网络调用超时限制*/, communicationMode/*rpc调用模式*/, pullCallback/*异步拉取结果回调*/);
    }

    private long recalculatePullFromWhichNode(final MessageQueue mq /*本次拉消息队列*/) {
        if (this.isConnectBrokerByUser()) { // 一般是 false
            return this.defaultBrokerId;
        }

        // 从推荐表中拿到该mq推荐主机id
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        // 否则从主节点上拿
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

    private int randomNum() {
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

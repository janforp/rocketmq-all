package org.apache.rocketmq.client.impl.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("all")
public class DefaultMQPushConsumerImpl implements MQConsumerInner {

    /**
     * Delay some time when exception occur
     */
    @Setter
    private long pullTimeDelayMillsWhenException;

    /**
     * Flow control interval
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;

    /**
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_SUSPEND = 1000;

    private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;

    private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;

    private final InternalLogger log = ClientLogger.getLog();

    // 门面
    @Getter
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    // 负载均衡

    /**
     * rbl 对象，职责：分配订阅主题的队列给当前消费者，20s一个周期执行 rbl 算法 （客户端实例触发）
     *
     * @see RebalanceService
     * @see RebalanceService#run()
     * @see MQClientInstance#doRebalance()
     * @see DefaultMQPushConsumerImpl#doRebalance()
     * @see RebalanceImpl#doRebalance(boolean)
     * @see RebalanceImpl#rebalanceByTopic(java.lang.String, boolean)
     */
    @Getter
    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);

    /**
     * 消息拉下来之后的钩子
     */
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    // 消费者启动时间
    private final long consumerStartTimestamp = System.currentTimeMillis();

    /**
     * 在消费的时候的钩子
     **/
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();

    private final RPCHook rpcHook;

    /**
     * 当前消费者服务主题
     * 启动正常：RUNNING
     */
    @Getter
    @Setter
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    /**
     * 客户端实例，整个进程内只有一个对象
     */
    @Setter
    private MQClientInstance mQClientFactory;

    /**
     * 拉消息逻辑的封装（为什么要封装？内部有推荐下次 pull 时 主机的算法，服务器 broker 返回结果中，包装着下次 pull 的时候推荐的 brokerId，根据本次请求数据的冷热来进行推荐）
     * 热数据：在内存中，会建议下次去 master 拉
     * 冷数据：在硬盘中，会建议下次去 slave 拉
     */
    private PullAPIWrapper pullAPIWrapper;

    // 是否暂停
    @Getter
    @Setter
    private volatile boolean pause = false;

    // 是否顺序消费
    @Getter
    @Setter
    private boolean consumeOrderly = false;

    /**
     * 消息监听器
     *
     * @see MessageListenerConcurrently
     * @see MessageListenerOrderly
     */
    @Getter
    private MessageListener messageListenerInner;

    /**
     * 消息进度存储器
     *
     * @see RemoteBrokerOffsetStore
     *
     * // 正常情况到这里执行
     * switch (this.defaultMQPushConsumer.getMessageModel()) {
     * case BROADCASTING:
     * // 广播模式
     * this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
     * break;
     * case CLUSTERING:
     * // 集群模式
     * this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
     * break;
     * default:
     * break;
     * @see DefaultMQPushConsumerImpl#start()
     */
    @Getter
    @Setter
    private OffsetStore offsetStore;

    /**
     * 消息消费服务，里面有消费线程池，执行消费任务
     *
     * @see ConsumeMessageConcurrentlyService
     * @see ConsumeMessageOrderlyService
     */
    @Getter
    @Setter
    private ConsumeMessageService consumeMessageService;

    /**
     * 队列流控次数（打印日志用，默认每1000次流控，进行一次日志打印）
     */
    private long queueFlowControlTimes = 0;

    // 流控，打印日志使用
    private long queueMaxSpanFlowControlTimes = 0;

    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        this.rpcHook = rpcHook;
        this.pullTimeDelayMillsWhenException = defaultMQPushConsumer.getPullTimeDelayMillsWhenException();
    }

    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    public boolean hasHook() {
        return !this.consumeMessageHookList.isEmpty();
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }

    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                    // ignore
                }
            }
        }
    }

    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    // ignore
                }
            }
        }
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        // ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable

        // 根据主题拿到该主题下的所有队列
        Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (null == result) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        }

        if (null == result) {
            throw new MQClientException("The topic[" + topic + "] not exist", null);
        }

        return parseSubscribeMessageQueues(result);
    }

    public Set<MessageQueue> parseSubscribeMessageQueues(Set<MessageQueue> messageQueueList) {
        Set<MessageQueue> resultQueues = new HashSet<MessageQueue>();
        for (MessageQueue queue : messageQueueList) {
            String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(), this.defaultMQPushConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
        }

        return resultQueues;
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    /**
     * 真正的拉数据
     *
     * @param pullRequest 请求
     */
    public void pullMessage(final PullRequest pullRequest) {
        // 获取拉消息队列的快照
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        if (processQueue.isDropped()) {
            // 如果删除了，则直接返回，不拉了，可能是 rbl 之后，该队列被转移到其他消费者了
            log.info("the pull request[{}] is dropped.", pullRequest.toString());
            return;
        }

        processQueue.setLastPullTimestamp(System.currentTimeMillis());

        try {
            // 检查状态
            this.makeSureStateOK();
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            // 如果当前消费者不是运行状态，延迟一段时间再次取拉消息
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException/*1000*/);
            return;
        }

        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }

        // 快照内缓存的消息数量
        AtomicLong msgCount = processQueue.getMsgCount();
        long cachedMessageCount = msgCount.get();
        // 快照内缓冲的消息大小
        AtomicLong msgSize = processQueue.getMsgSize();
        long cachedMessageSizeInMiB = msgSize.get() / (1024 * 1024);

        // 按照消息数量流控
        if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()/*1000*/) {
            // 如果 快照内缓冲的消息数量 大于 阈值，则本次拉取任务需要延迟50ms

            // 延迟一段时间之后再拉消息
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL/*50ms*/);
            return;
        }

        // 按照消息大小流控
        if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()/*100m*/) {
            // 延迟一段时间之后再拉消息
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL/*50ms*/);
            return;
        }

        if (!this.consumeOrderly) {
            // 并发消费

            // 第一条消息跟最后一条消息的偏移量差 > 2000,则进行流控
            if (processQueue.getMaxSpan()/*msgTreeMap 中的 偏移量差*/ > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()/*2000*/) {
                // 延迟一段时间之后再拉消息
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL/*50ms*/);
                return;
            }
        } else {
            // 顺序消费
            if (processQueue.isLocked()) {
                if (!pullRequest.isLockedFirst()) {
                    final long offset = this.rebalanceImpl.computePullFromWhere(pullRequest.getMessageQueue());
                    boolean brokerBusy = offset < pullRequest.getNextOffset();
                    log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}", pullRequest, offset, brokerBusy);
                    if (brokerBusy) {
                        log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}", pullRequest, offset);
                    }

                    pullRequest.setLockedFirst(true);
                    pullRequest.setNextOffset(offset);
                }
            } else {
                this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException/*1000*/);
                log.info("pull message later because not locked in broker, {}", pullRequest);
                return;
            }
        }

        // 拿到该主题的订阅信息
        String topic = pullRequest.getMessageQueue().getTopic();

        // ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner
        ConcurrentMap<String/* topic */, SubscriptionData> subscriptionInner = this.rebalanceImpl.getSubscriptionInner();
        final SubscriptionData subscriptionData = subscriptionInner.get(topic);
        if (null == subscriptionData) {
            // 何时 subscriptionData 会是 null 呢？unsubscribe 的时候。会删除该主题，这个时候该条件会成立
            /*
             * 后面的逻辑交给了负载均衡
             * 最终rbl程序会对比订阅集合。会将移除的订阅主题的 processQueue 设置为 true ，然后该 queue 对于的 pullRequest 请求就会退出了
             */

            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException/*1000*/);
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        // 正常情况

        final long beginTimestamp = System.currentTimeMillis();

        //  PullCallback   开始

        // 从 broker 拉取消息接口返回之后，会回调这个对象中的2个方法！！！
        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult /* 拉消息的结果 */) {
                if (pullResult != null) {
                    final MessageQueue messageQueue = pullRequest.getMessageQueue();
                    pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult/*主要是反序列化消息以及根据消息tag过滤*/(messageQueue, pullResult, subscriptionData);
                    //    FOUND,
                    //    NO_NEW_MSG,
                    //    NO_MATCHED_MSG,
                    //    OFFSET_ILLEGAL
                    PullStatus pullStatus = pullResult.getPullStatus();
                    long nextBeginOffset = pullResult.getNextBeginOffset();
                    switch (pullStatus) {
                        /**
                         * 拉取消息成功处理步骤
                         * 1.更新 pullRequest 的 nextOffset = pullResult.getNextBeignOffset();
                         * 2.将拉取下来的消息加入到本地该mq对应的 processQueue 快照中
                         * 3.将拉取下来的消息以及 processQueue，messageQueue 提交到 ConsumeMessageService ，内部会封装成 消费服务 提交到消费服务线程池中处理
                         * 4.再次将 pullRequest 对象提交到 PullMessageService 的任务队列中，形成拉消息闭环
                         */
                        case FOUND: // 拿到消息
                            long prevRequestOffset = pullRequest.getNextOffset();

                            // 设置下次拉消息的 offset，如果被u更新，下次会拉取到重复到消息
                            pullRequest.setNextOffset(nextBeginOffset /*因为这个拉消息的请求对象是循环使用的(DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest))，本次拉取完成之后任务会继续使用该对象取进行下一次拉取，肯定要设置成下次拉取消息的offset！*/);
                            long pullRT = System.currentTimeMillis() - beginTimestamp;// 本次拉消息的 response time
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(), messageQueue.getTopic(), pullRT);

                            long firstMsgOffset = Long.MAX_VALUE;
                            List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
                            if (msgFoundList == null || msgFoundList.isEmpty()) {
                                // 进来这里一般是因为：客户端这边过滤了，导致消息为空了
                                // 则告诉拉消息服务继续拉
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            } else {
                                // 一般进来这里，过滤没有全部过滤

                                // 拉到消息的第一条消息的偏移量
                                firstMsgOffset = msgFoundList.get(0).getQueueOffset();

                                DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(), messageQueue.getTopic(), msgFoundList.size());

                                // 把拉取到的消息设置到本地快照中，等待后面的消费
                                boolean dispatchToConsume = processQueue.putMessage(msgFoundList);

                                // TODO 提交消费任务！！！！！！！！！
                                ConsumeMessageService consumeMessageService = DefaultMQPushConsumerImpl.this.consumeMessageService;
                                consumeMessageService.submitConsumeRequest(msgFoundList /*从服务器端拉取下来的并且客户端再次过滤后的消息 */, processQueue, messageQueue, dispatchToConsume /*并发消费服务忽略这个参数*/);

                                // 再次将 'pullRequest（更新部分字段如 nextBeginOffset之后的对象）' 对象提交到 拉取消息服务，这样就形成拉一个拉消息的闭环
                                if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                    // 配置拉取间隔时间
                                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                                } else {
                                    // 无间隔时间
                                    DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                }
                            }

                            if (nextBeginOffset < prevRequestOffset || firstMsgOffset < prevRequestOffset) {
                                log.warn("[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}", nextBeginOffset, firstMsgOffset, prevRequestOffset);
                            }

                            break;
                        case NO_NEW_MSG:// 没有查询到消息
                        case NO_MATCHED_MSG://查询到了，但是在客户端被过滤掉的情况
                            // 也更新下次拉取的偏移量
                            pullRequest.setNextOffset(nextBeginOffset);
                            // 更新最新的 offset
                            DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);
                            // 再次将 'pullRequest（更新部分字段如 nextBeginOffset之后的对象）' 对象提交到 拉取消息服务，这样就形成拉一个拉消息的闭环
                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            break;
                        case OFFSET_ILLEGAL: // 传给服务端的 offset 不对（offset > maxOffset || offset < minOffset），服务器会推荐一个合理的 offset
                            log.warn("the pull request offset illegal, {} {}", pullRequest.toString(), pullResult.toString());

                            // 调整 pullRequest的 nextOffset 为正确的
                            pullRequest.setNextOffset(nextBeginOffset);

                            // 删除快照
                            pullRequest.getProcessQueue().setDropped(true);

                            // 10s 后执行一个任务
                            DefaultMQPushConsumerImpl.this.executeTaskLater(new Runnable() {
                                // 持久化该mq的消费进度到broker
                                @Override
                                public void run() {
                                    try {
                                        DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(messageQueue, pullRequest.getNextOffset(), false);

                                        // 持久化该队列的消费进度到服务器 broker
                                        DefaultMQPushConsumerImpl.this.offsetStore.persist(messageQueue);

                                        /**
                                         * 消费者本地删除状态
                                         * 该队列岂不是没有消费者去消费拉？
                                         * 答：是的，但是很快就会有消费者去消费拉，rbl(每20秒执行一次)程序会再次为该队列创建新的 processQueue，再次
                                         * 为该队列创建 pullRequest ，该 pullRequest 会再次被加入到 PullMessageServie 任务队列
                                         */
                                        DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(messageQueue);

                                        // 并没有再次提交 pullRequest 到拉取消息服务，负载均衡服务会再次分配该队列，并极有可能拿到正确的 offset

                                        log.warn("fix the pull request offset, {}", pullRequest);
                                    } catch (Throwable e) {
                                        log.error("executeTaskLater Exception", e);
                                    }
                                }
                            }, 10000);
                            break;
                        default:
                            break;
                    }
                }
            }

            @Override
            public void onException(Throwable e) {
                if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception", e);
                }
                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            }
        };

        //  PullCallback   结束

        // 是否提交消费者本地该队列的 offset
        boolean commitOffsetEnable = false;

        // 该队列在消费者本地的 offset
        long commitOffsetValue = 0L;
        if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
            commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
            if (commitOffsetValue > 0) {
                commitOffsetEnable = true;
            }
        }

        // 过滤表达式
        String subExpression = null;
        // 是否类过滤模式
        boolean classFilter = false;
        // 该主题的订阅数据
        SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) {
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() /* 是否在拉数据的时候提交订阅数据？ */ && !sd.isClassFilterMode()) {
                subExpression = sd.getSubString();
            }

            classFilter = sd.isClassFilterMode();
        }

        // 如果全都是 true，则 flag 为：0    0   0   0   1(是否为类过滤，，默认0，一般是TAG过滤)   1(拉消息请求是否包含消费者本地该主题的订阅信息，默认0，因为心跳的时候做了这个事情)   1(是否允许服务器长轮询，默认1)   1(是否提交消费者本地的进度,如果为1则表示提交，默认1)
        // 高四位未使用
        int sysFlag = PullSysFlag.buildSysFlag(commitOffsetEnable, true, subExpression != null, classFilter);
        try {
            this.pullAPIWrapper.pullKernelImpl(
                    pullRequest.getMessageQueue(), // 队列
                    subExpression, // 过滤表达式，一般是null
                    subscriptionData.getExpressionType(), // 表达式类型，一般是 TAG
                    subscriptionData.getSubVersion(), // 客户端版本
                    pullRequest.getNextOffset(), // 本次拉取消息的偏移量
                    this.defaultMQPushConsumer.getPullBatchSize(), // 批量大小
                    sysFlag, // 如果全都是 true，则 flag 为：0    0   0   0   1   1   1   1
                    commitOffsetValue, // 本地该队列的消费进度
                    BROKER_SUSPEND_MAX_TIME_MILLIS, // 控制服务器端长轮询的时候，最长hold的时间，15秒
                    CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND, // RPC 超时时间，网络调用超时数据限制30s
                    CommunicationMode.ASYNC, // 异步
                    pullCallback // 消息拉回来之后的回调
            );
        } catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
        }
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, " + this.serviceState + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
        }
    }

    // 延迟一段时间之后再拉消息
    private void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        PullMessageService pullMessageService = this.mQClientFactory.getPullMessageService();

        pullMessageService.executePullRequestLater(pullRequest, timeDelay);
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.mQClientFactory.getConsumerStatsManager();
    }

    public void executePullRequestImmediately(final PullRequest pullRequest) {
        PullMessageService pullMessageService = this.mQClientFactory.getPullMessageService();
        pullMessageService.executePullRequestImmediately(pullRequest);
    }

    private void correctTagsOffset(final PullRequest pullRequest) {
        if (0L == pullRequest.getProcessQueue().getMsgCount().get() /*当前对象存储消息的条数*/) {
            this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }

    public void resume() {
        this.pause = false;
        doRebalance();
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName) : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg, this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());
        } catch (Exception e) {
            log.error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e);
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());

            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());
            this.mQClientFactory.getDefaultMQProducer().send(newMsg);
        } finally {
            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
        }
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: 16
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return 16;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    public synchronized void shutdown() {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.consumeMessageService.shutdown();
                this.persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
                this.mQClientFactory.shutdown();
                log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.destroy();
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                // 先设置失败
                this.serviceState = ServiceState.START_FAILED;
                // 检查配置
                this.checkConfig();
                // 复制订阅信息到 rebalanceImpl 对象，并且如果是集群模式的时候，系统自动订阅 重试 主题队列
                this.copySubscription();
                if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    // 集群模式，修改消费者实例名称为 进程 PID
                    this.defaultMQPushConsumer.changeInstanceNameToPID();
                }

                // 获取客户端实例，进程内只有一个实例
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);

                // 初始化负载均衡对象
                this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
                // 分配策略对象赋值
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                // 初始化 pullAPIWrapper 对象
                /*
                 * 拉消息逻辑的封装（为什么要封装？内部有推荐下次 pull 时 主机的算法，服务器 broker 返回结果中，包装着下次 pull 的时候推荐的 brokerId，根据本次请求数据的冷热来进行推荐）
                 * 热数据：在内存中，会建议下次去 master 拉
                 * 冷数据：在硬盘中，会建议下次去 slave 拉
                 */
                this.pullAPIWrapper = new PullAPIWrapper(mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());

                // 注册到这里， 消息拉取到的时候执行这个钩子
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                if (this.defaultMQPushConsumer.getOffsetStore() != null) {
                    // 一般情况下，不会进入这个分支
                    this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
                } else {
                    // 正常情况到这里执行
                    switch (this.defaultMQPushConsumer.getMessageModel()) {
                        case BROADCASTING:
                            // 广播模式
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING:
                            // 集群模式
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                    // 然后再回写给上层的门面
                    this.defaultMQPushConsumer.setOffsetStore(this.offsetStore);
                }

                // 集群模式下，啥都没干
                this.offsetStore.load();

                // 创建 consumeMessageService 对象
                // 根据监听器的区别有所不同
                if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                    this.consumeOrderly = true;
                    this.consumeMessageService = new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                } else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                    // 使用较多的
                    this.consumeOrderly = false;
                    this.consumeMessageService = new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                }

                // 启动消费服务
                this.consumeMessageService.start();
                /**
                 * 将消费者注册到客户端实例
                 * 为何注册：客户端实例给消费者提供了什么服务？
                 * 1.心跳
                 * 2.拉消息
                 * 3.队列负载
                 * 4.消息进度持久化
                 * 5.动态调整线程池
                 */
                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    // 不能重复注册消费者
                    this.serviceState = ServiceState.CREATE_JUST;
                    this.consumeMessageService.shutdown();
                    throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup() + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
                }

                // 启动客户端实例
                mQClientFactory.start();
                log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());

                // 修改消费者状态为运行中
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PushConsumer service state not OK, maybe started once, " + this.serviceState + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
            default:

                // CASE 执行完成，继续往下
                break;
        }

        // 路由更新服务
        // 从 namesrv 强制加载 主题路由 数据，生产 主题的 Set<MessageQueue> 交给 rbl 中的 table
        this.updateTopicSubscribeInfoWhenSubscriptionChanged();

        // 检查服务器是否支持 "消息过滤模式"（一般都是 tag 过滤）如果不支持，则抛出异常
        /**
         * 主要是校验当前订阅的主题所在的 broker 是否支持 表达式类型
         * 因为不是每个 broker 都支持 除 TAG 之外的过滤类型的
         */
        this.mQClientFactory.checkClientInBroker();
        // 向所有已知的 broker 节点 发送心态，主要包括 订阅信息数据
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
        // 唤醒负载均衡线程，让 rbl 线程立马执行负载均衡的业务，负载均衡之后如果订阅关系发送改变也会立即发送心跳到broker
        this.mQClientFactory.rebalanceImmediately();
    }

    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQPushConsumer.getConsumerGroup());

        if (null == this.defaultMQPushConsumer.getConsumerGroup()) {
            throw new MQClientException("consumerGroup is null" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        if (this.defaultMQPushConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException("consumerGroup can not equal " + MixAll.DEFAULT_CONSUMER_GROUP + ", please specify another one." + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        if (null == this.defaultMQPushConsumer.getMessageModel()) {
            throw new MQClientException("messageModel is null" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        if (null == this.defaultMQPushConsumer.getConsumeFromWhere()) {
            throw new MQClientException("consumeFromWhere is null" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        Date dt = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS);
        if (null == dt) {
            throw new MQClientException("consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received " + this.defaultMQPushConsumer.getConsumeTimestamp() + " " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // allocateMessageQueueStrategy
        if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException("allocateMessageQueueStrategy is null" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // subscription
        if (null == this.defaultMQPushConsumer.getSubscription()) {
            throw new MQClientException("subscription is null" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // messageListener
        if (null == this.defaultMQPushConsumer.getMessageListener()) {
            throw new MQClientException("messageListener is null" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        boolean orderly = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
        boolean concurrently = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
        if (!orderly && !concurrently) {
            throw new MQClientException("messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // consumeThreadMin
        if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1 || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000) {
            throw new MQClientException("consumeThreadMin Out of range [1, 1000]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1 || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
            throw new MQClientException("consumeThreadMax Out of range [1, 1000]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // consumeThreadMin can't be larger than consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer.getConsumeThreadMax()) {
            throw new MQClientException("consumeThreadMin (" + this.defaultMQPushConsumer.getConsumeThreadMin() + ") " + "is larger than consumeThreadMax (" + this.defaultMQPushConsumer.getConsumeThreadMax() + ")", null);
        }

        // consumeConcurrentlyMaxSpan
        if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1 || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
            throw new MQClientException("consumeConcurrentlyMaxSpan Out of range [1, 65535]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // pullThresholdForQueue
        if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
            throw new MQClientException("pullThresholdForQueue Out of range [1, 65535]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // pullThresholdForTopic
        if (this.defaultMQPushConsumer.getPullThresholdForTopic() != -1) {
            if (this.defaultMQPushConsumer.getPullThresholdForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdForTopic() > 6553500) {
                throw new MQClientException("pullThresholdForTopic Out of range [1, 6553500]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
            }
        }

        // pullThresholdSizeForQueue
        if (this.defaultMQPushConsumer.getPullThresholdSizeForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForQueue() > 1024) {
            throw new MQClientException("pullThresholdSizeForQueue Out of range [1, 1024]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() != -1) {
            // pullThresholdSizeForTopic
            if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForTopic() > 102400) {
                throw new MQClientException("pullThresholdSizeForTopic Out of range [1, 102400]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
            }
        }

        // pullInterval
        if (this.defaultMQPushConsumer.getPullInterval() < 0 || this.defaultMQPushConsumer.getPullInterval() > 65535) {
            throw new MQClientException("pullInterval Out of range [0, 65535]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // consumeMessageBatchMaxSize
        if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1 || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
            throw new MQClientException("consumeMessageBatchMaxSize Out of range [1, 1024]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // pullBatchSize
        if (this.defaultMQPushConsumer.getPullBatchSize() < 1 || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
            throw new MQClientException("pullBatchSize Out of range [1, 1024]" + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }
    }

    /**
     * 复制订阅信息到 rebalanceImpl 对象，并且如果是集群模式的时候，系统自动订阅 重试 主题队列
     *
     * @throws MQClientException
     */
    private void copySubscription() throws MQClientException {
        try {
            // Map<String /* topic */, String /* sub expression */> subscription = new HashMap<String, String>();
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            if (sub != null) {
                for (final Map.Entry<String, String> entry : sub.entrySet()) {
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();
                    // 组装得到一个订阅信息对象
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), topic, subString);

                    // 拷贝存储到负载均衡这个对象中
                    ConcurrentMap<String, SubscriptionData> subscriptionInner = this.rebalanceImpl.getSubscriptionInner();
                    //  ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner
                    subscriptionInner.put(topic, subscriptionData);
                }
            }

            if (null == this.messageListenerInner) {
                this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
            }

            /**
             * 如果是集群模式，则系统字段订阅 %RETRY%SOCINSCORE_CONSUMER_GROUP 重试主题队列
             */
            if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {

                // %RETRY%SOCINSCORE_CONSUMER_GROUP
                // 消息重试的主题，因为会有消费失败的情况
                final String retryTopic/* %RETRY%SOCINSCORE_CONSUMER_GROUP */ = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                // 创建该主题的订阅数据对象
                SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), retryTopic, SubscriptionData.SUB_ALL/* * */);

                // 存储到负载均衡服务对象中
                ConcurrentMap<String, SubscriptionData> subscriptionInner = this.rebalanceImpl.getSubscriptionInner();

                // 存储到负载均衡服务对象中
                /*
                 * 这里为什么要订阅重试主题呢？
                 * 消息重试时，消息最终会再次加入到该主题，消费者订阅这个主题之后，就会再次拿到该消息，可以再次处理
                 */
                subscriptionInner.put(retryTopic, subscriptionData);
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        /**
         * ConcurrentMap<String , SubscriptionData> subscriptionInner
         * @see  RebalanceImpl#subscriptionInner
         */
        Map<String /* topic */, SubscriptionData/*topic的订阅信息*/> subTable = this.getSubscriptionInner();
        if (subTable == null) {
            return;
        }
        for (final Map.Entry<String /* topic */, SubscriptionData> entry : subTable.entrySet()) {
            final String topic = entry.getKey();
            // 更新路由数据,并且更新本地路由数据
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return this.rebalanceImpl.getSubscriptionInner();
    }

    /**
     * 把订阅信息塞入本地的映射表，并且马上发送心跳给broker
     */
    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            String consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(consumerGroup, topic, subExpression);
            ConcurrentMap<String, SubscriptionData> subscriptionInner = this.rebalanceImpl.getSubscriptionInner();
            subscriptionInner.put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /**
     * 把订阅信息塞入本地的映射表，并且马上发送心跳给broker
     */
    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        try {
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPushConsumer.getConsumerGroup(), topic, "*");
            subscriptionData.setSubString(fullClassName);
            subscriptionData.setClassFilterMode(true);
            subscriptionData.setFilterClassSource(filterClassSource);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }

        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /**
     * 把订阅信息塞入本地的映射表，并且马上发送心跳给broker
     */
    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        try {
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }

            SubscriptionData subscriptionData = FilterAPI.build(topic, messageSelector.getExpression(), messageSelector.getType());

            // 塞入订阅表
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void suspend() {
        this.pause = true;
        log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    public void unsubscribe(String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.offsetStore.updateOffset(mq, offset, false);
    }

    public void updateCorePoolSize(int corePoolSize) {
        this.consumeMessageService.updateCorePoolSize(corePoolSize);
    }

    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public void resetOffsetByTimeStamp(long timeStamp) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
            Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
            if (mqs != null) {
                for (MessageQueue mq : mqs) {
                    long offset = searchOffset(mq, timeStamp);
                    offsetTable.put(mq, offset);
                }
                this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
            }
        }
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    @Override
    public String groupName() {
        return this.defaultMQPushConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultMQPushConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        // CONSUME_PASSIVELY("PUSH");
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return this.defaultMQPushConsumer.getConsumeFromWhere();
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        return new HashSet<SubscriptionData>(this.rebalanceImpl.getSubscriptionInner().values());
    }

    /**
     * @see RebalanceService 服务每隔20s调用一次
     */
    @Override
    public void doRebalance() {
        if (!this.pause) {
            boolean consumeOrderly = this.isConsumeOrderly();
            this.rebalanceImpl.doRebalance(consumeOrderly);
        }
    }

    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();

            ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = this.rebalanceImpl.getProcessQueueTable();

            Set<MessageQueue> allocateMq = processQueueTable.keySet();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>(allocateMq);

            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPushConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
        }
    }

    /**
     * 修改主题订阅信息
     *
     * @param topic
     * @param info
     */
    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {

        // 主题订阅信息
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQPushConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);

        prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, String.valueOf(this.consumeOrderly));
        prop.put(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE, String.valueOf(this.consumeMessageService.getCorePoolSize()));
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));

        info.setProperties(prop);

        Set<SubscriptionData> subSet = this.subscriptions();
        info.getSubscriptionSet().addAll(subSet);

        for (Entry<MessageQueue, ProcessQueue> next : this.rebalanceImpl.getProcessQueueTable().entrySet()) {
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            ProcessQueueInfo pqinfo = new ProcessQueueInfo();
            pqinfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
            pq.fillProcessQueueInfo(pqinfo);
            info.getMqTable().put(mq, pqinfo);
        }

        for (SubscriptionData sd : subSet) {
            ConsumeStatus consumeStatus = this.mQClientFactory.getConsumerStatsManager().consumeStatus(this.groupName(), sd.getTopic());
            info.getStatusTable().put(sd.getTopic(), consumeStatus);
        }

        return info;
    }

    public void adjustThreadPool() {
        long computeAccTotal = this.computeAccumulationTotal();
        long adjustThreadPoolNumsThreshold = this.defaultMQPushConsumer.getAdjustThreadPoolNumsThreshold();

        long incThreshold = (long) (adjustThreadPoolNumsThreshold * 1.0);

        long decThreshold = (long) (adjustThreadPoolNumsThreshold * 0.8);

        if (computeAccTotal >= incThreshold) {
            this.consumeMessageService.incCorePoolSize();
        }

        if (computeAccTotal < decThreshold) {
            this.consumeMessageService.decCorePoolSize();
        }
    }

    private long computeAccumulationTotal() {
        long msgAccTotal = 0;
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = this.rebalanceImpl.getProcessQueueTable();
        for (Entry<MessageQueue, ProcessQueue> next : processQueueTable.entrySet()) {
            ProcessQueue value = next.getValue();
            msgAccTotal += value.getMsgAccCnt();
        }

        return msgAccTotal;
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        List<QueueTimeSpan> queueTimeSpan = new ArrayList<QueueTimeSpan>();
        TopicRouteData routeData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            String addr = brokerData.selectBrokerAddr();
            queueTimeSpan.addAll(this.mQClientFactory.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic, groupName(), 3000));
        }

        return queueTimeSpan;
    }

    public void resetRetryAndNamespace(final List<MessageExt> msgs, String consumerGroup) {
        // %RETRY% + consumerGroup;
        // 当前消费者组的重试主题
        final String groupTopic = MixAll.getRetryTopic(consumerGroup);
        for (MessageExt msg : msgs) {
            // 重试主题，原主题，只有被重复消费的消息才有该属性，一般的消息是没有该属性的
            String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
            if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                // 说明当前消息是被重复消费的消息
                // 那么需要把主题重新设置为原主题
                msg.setTopic(retryTopic);
            }

            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }
}

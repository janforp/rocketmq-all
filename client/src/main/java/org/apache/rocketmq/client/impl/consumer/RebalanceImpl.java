package org.apache.rocketmq.client.impl.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@SuppressWarnings("all")
public abstract class RebalanceImpl {

    protected static final InternalLogger log = ClientLogger.getLog();

    /**
     * 分配到当前消费者的队列消息
     */
    @Getter
    protected final ConcurrentMap<MessageQueue/*分配给当前消费者的队列*/, ProcessQueue/*队列在消费者的快照*/> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);

    /**
     * 初始化地方
     * 1.客户端启动阶段
     * 2.定时任务(30s)
     */
    @Getter
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>/*该主题的队列发布信息 {"topic":"topic","brokerName":"broker-a",queueId:"0"}*/> topicSubscribeInfoTable = new ConcurrentHashMap<String, Set<MessageQueue>>();

    /**
     * @see DefaultMQPushConsumerImpl#copySubscription() 该方法初始化该 table
     *
     * 当前消费者的订阅信息
     * key:当前消费者订阅的主题
     * value:订阅详情
     * @see DefaultMQPushConsumerImpl#copySubscription() 消费者实例启动的时候会把它的订阅信息复制过来
     * @see DefaultMQPushConsumerImpl#subscribe(java.lang.String, java.lang.String) 消费者订阅主题的是也会把数据放到这里
     */
    @Getter
    protected final ConcurrentMap<String /* topic */, SubscriptionData/*订阅信息，包括 topic 以及过滤信息*/> subscriptionInner = new ConcurrentHashMap<String, SubscriptionData>();

    @Getter
    @Setter
    protected String consumerGroup;

    // 广播 还是 集群
    @Getter
    @Setter
    protected MessageModel messageModel;

    /**
     * 队列分配策略
     */
    @Getter
    @Setter
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    @Getter
    protected MQClientInstance mQClientFactory;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel, AllocateMessageQueueStrategy allocateMessageQueueStrategy, MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        HashMap<String/*brokerName*/, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String/*brokerName*/, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty()) {
                continue;
            }

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    String brokerAddr = findBrokerResult.getBrokerAddr();
                    MQClientAPIImpl mqClientAPIImpl = this.mQClientFactory.getMQClientAPIImpl();
                    mqClientAPIImpl.unlockBatchMQ(brokerAddr, requestBody, 1000, oneway);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    // 将分配给当前消费者的全部mq,按 brokerName 分组
    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String/* brokerName */, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();

        // ConcurrentMap<MessageQueue/*分配给当前消费者的队列*/, ProcessQueue/*队列在消费者的快照*/> processQueueTable
        Set<MessageQueue> messageQueueSet = this.processQueueTable.keySet();
        for (MessageQueue mq : messageQueueSet) {
            String brokerName = mq.getBrokerName();
            Set<MessageQueue> mqs = result.get(brokerName);
            if (null == mqs) {
                mqs = new HashSet<MessageQueue>();
                result.put(brokerName, mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    public boolean lock(final MessageQueue mq) {
        String brokerName = mq.getBrokerName();
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                MQClientAPIImpl mqClientAPIImpl = this.mQClientFactory.getMQClientAPIImpl();
                String brokerAddr = findBrokerResult.getBrokerAddr();
                // ConcurrentMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable = new ConcurrentHashMap<>(1024);
                /**
                 * @see org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager#tryLockBatch(java.lang.String, java.util.Set, java.lang.String)
                 * @see org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager.LockEntry
                 */
                Set<MessageQueue> lockedMq = mqClientAPIImpl.lockBatchMQ(brokerAddr, requestBody, 1000);
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                boolean lockOK = lockedMq.contains(mq);
                log.info("the message queue lock {}, {} {}", lockOK ? "OK" : "Failed", this.consumerGroup, mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    public void lockAll() {

        // 将分配给当前消费者的全部mq,按 brokerName 分组
        HashMap<String /* brokerName */ , Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        // 循环处理每一个 brokerName 组
        for (Entry<String/* brokerName */, Set<MessageQueue>> entry : brokerMqs.entrySet()) {

            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty()) {
                continue;
            }

            // 查询broker主节点信息
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {

                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {

                    // 同步网络调用，返回续约锁成功的队列集合
                    MQClientAPIImpl mqClientAPIImpl = this.mQClientFactory.getMQClientAPIImpl();
                    String brokerAddr = findBrokerResult.getBrokerAddr();

                    // 发起续约锁的请求到master节点
                    Set<MessageQueue> lockOKMQSet = mqClientAPIImpl.lockBatchMQ(brokerAddr, requestBody, 1000);

                    // 更新续约成功的 pd 属性
                    for (MessageQueue mq : lockOKMQSet) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq/*在服务端续约锁成功的队列*/);
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }

                            // 更新续约成功的 pd 属性
                            processQueue.setLocked(true);
                            // 保存续约锁的时间
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }

                    // 续约锁失败的 pd 处理
                    for (MessageQueue mq : mqs) {
                        if (!lockOKMQSet.contains(mq)) {

                            // 找到续约失败的队列

                            ProcessQueue processQueue = this.processQueueTable.get(mq/*在服务端续约锁失败的队列*/);
                            if (processQueue != null) {
                                // 表示续约锁失败了，表示分布式锁尚未占用成功，消费任务不能消费
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    // 会给重平衡的结果通过心跳发送给 broker
    public void doRebalance(final boolean isOrder) {
        /*
         * 当前消费者的订阅信息
         * key:当前消费者订阅的主题
         * value:订阅详情
         *
         * ConcurrentMap<String * topic *, SubscriptionData> subscriptionInner
         */
        Map<String /* topic */, SubscriptionData/*订阅信息，包括 topic 以及过滤信息*/> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            // 遍历消费者订阅的每一个主题
            for (final Map.Entry<String /* topic */, SubscriptionData/*订阅信息，包括 topic 以及过滤信息*/> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    // 按照主题进行负载均衡
                    this.rebalanceByTopic(topic, isOrder/* 是否顺序 */);
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }
            }
        }
        this.truncateMessageQueueNotMyTopic();
    }

    /**
     * 按照主题进行负载均衡
     *
     * @param topic 主题
     * @param isOrder 是否顺序
     */
    private void rebalanceByTopic(final String topic, final boolean isOrder) {
        switch (messageModel) {
            case BROADCASTING: {
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                    }
                } else {
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            case CLUSTERING: {
                // ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable
                // 获取当前主题的全部 MessageQueue
                Set<MessageQueue> mqSet/*该主题所有的队列*/ = this.topicSubscribeInfoTable/*主题分布*/.get(topic);
                // 获取当前'消费者组'下的全部消费者ID（客户端实例ID集合）
                List<String> cidAll/*该消费者组下所有的消费者实例id*/ = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX/*%RETRY%*/)) {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                if (mqSet != null && cidAll != null) {
                    List<MessageQueue> mqAll = new ArrayList<MessageQueue>(mqSet /* 该主题的所有队列 */);

                    // 主题的 mq集合，消费者ID集合，都进行排序，目的是：每个消费者视图一致性
                    Collections.sort(mqAll/* 该主题的所有队列 */);
                    Collections.sort(cidAll/* 该组下面订阅该主题的所有消费者实例 id */);
                    // 队列分配策略
                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    // 负载均衡分配结果
                    List<MessageQueue> allocateResult;
                    try {
                        String clientId/*10.201.13.28@67717*/ = this.mQClientFactory.getClientId();
                        // 调用队列分配策略的分配方法
                        allocateResult = strategy.allocate(this.consumerGroup, clientId, mqAll, cidAll);
                    } catch (Throwable e) {
                        // log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(), e);
                        return;
                    }

                    // 用于装当前实例分配队列结果
                    Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                    if (allocateResult != null) {
                        // 得到本次分配到的 MessageQueue 集合
                        allocateResultSet.addAll(allocateResult);
                    }

                    // 0 1 2 3
                    //     2 3 4
                    // 分配到的队列发生了改变，新分配到的队列为4，0，1队列分配到其他消费者实例上了，需移除，其中2，3队列没有发生变化

                    // 更新当前消费者实例的队列消息
                    boolean changed /*true表示分配给当前消费者的队列发生变化，false没有变化,当分配的队列多了或者少了的时候都会发生变化*/ = this.updateProcessQueueTableInRebalance(topic /*主题*/, allocateResultSet/*当前消费者的分配结果*/, isOrder);
                    if (changed) {

                        // 会给重平衡的结果通过心跳发送给 broker
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    /**
     * 以订阅信息{@link RebalanceImpl#subscriptionInner}为准,从 {@link RebalanceImpl#processQueueTable} 处理队列中删除没有订阅的队列
     */
    private void truncateMessageQueueNotMyTopic /* 截断消息队列不是我的主题 */() {
        Map<String /* topic */, SubscriptionData/*订阅信息，包括 topic 以及过滤信息*/> subTable = this.getSubscriptionInner();

        for (MessageQueue mq/*分配给当前消费者的队列*/ : this.processQueueTable.keySet()) {

            String topic = mq.getTopic();
            if (!subTable.containsKey(topic) /*意思是当前消费者的订阅信息中没有订阅这个主题*/) {
                // 当前消费者的订阅信息中没有订阅某个主题的时候，肯定要把这里的映射表删除！！！
                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }

    /**
     * // 负载均衡前： 0 1 2 3
     * // 负载均衡后：     2 3 4
     * // 分配到的队列发生了改变，新分配到的队列为4，队列 0，1分配到其他消费者实例上了，需移除，其中2，3队列没有发生变化
     *
     * 计数出负载均衡后，当前消费者，当前主题被转移走的队列
     * 对于这些被转移到其他消费者的队列，当前消费者需要：
     * 1.将 mq > pd 状态设置为 删除 状态
     * 2.持久化消息进度 + 删除本地该 mq 的消费进度
     * 3.processQueueTable 中删除该条 k-v
     *
     * 计数出本次负载均衡后，新分配到当前消费者该主题的队列
     * 对于这些新分配到当前消费者的该主题的队列，需要做：
     * 1.创建 ProcessQueue 为每个新分配队列
     * 2.获取新分配队列的消费进度（offset）,获取方式：到队列归属 broker 上拉取
     * 3.processQueueTable 添加 k-v ，key: messageQueue,value: processQueue
     * 4.为新分配队列，创建 {@link PullRequest} 对象（封装：消费者组，mq,pd,消费进度）
     * 5.上一步创建的 PullRequest 对象转交给 PullMessageService （拉取消息服务）
     *
     *
     *
     * 总结：
     * 1.从 {@link RebalanceImpl#processQueueTable} 映射表中移除分配走的队列
     * 2.给新分配的队列以及拉消息超时的旧队列创建新的 {@link ProcessQueue}
     * 3.针对哪些需要从映射表 {@link RebalanceImpl#processQueueTable} 移除的队列会进行消费进度的持久化
     *
     * @param topic 主题
     * @param newMqSet 最新分配给消费者的当前主题的队列集合
     * @param isOrder 是否顺序消费
     */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> newMqSet/*该主题在该实例上新分配的队列*/, final boolean isOrder) {

        // 当前消费者 消费的队列是否有变化
        boolean changed = false;

        // ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable，这些队列是本次负载均衡之前的队列，但是这个队列中保存的是所有主题下的队列
        Iterator<Entry<MessageQueue/*之前分配给当前消费者的队列*/, ProcessQueue>> it = this.processQueueTable/* 本次负载均衡之前的处理队列 */.entrySet().iterator();
        while (it.hasNext() /* 遍历该主题在该实例上老队列 */) {
            Entry<MessageQueue, ProcessQueue> next = it.next();

            // 队列
            MessageQueue mq = next.getKey();
            // 队列在消费者端的快照
            ProcessQueue pq = next.getValue();

            if (mq.getTopic().equals(topic) /* 只处理当前 主题的  队列，因为该消费者可能还订阅了其他的主题！！！ */) {
                // 找到传入主题对应的 MessageQueue

                if (!newMqSet/*该主题在该实例上新分配的队列*/.contains(mq) /* 说明 之前的旧 mq 已经被分配给其他消费者了，需要移除 */) {
                    // 最新分配给消费者的当前主题的队列集合 中已经没有了当前循环的 mq，说明该mq经过rbl计算之后已经分配给其他消费者节点了
                    // 需要把该 mq 在当前消费者的快照设置为 删除
                    // 消费任务会一直检查 dropped 状态，如果是删除，则立马退出
                    /**
                     *        if (processQueue.isDropped()) {
                     *             // 如果删除了，则直接返回，不拉了，可能是 rbl 之后，该队列被转移到其他消费者了
                     *             log.info("the pull request[{}] is dropped.", pullRequest.toString());
                     *             return;
                     *         }
                     * @see DefaultMQPushConsumerImpl#pullMessage(org.apache.rocketmq.client.impl.consumer.PullRequest) 拉消息的时候会判断该字段是否为false
                     */
                    pq.setDropped(true/*删除*/);

                    /*
                     * 1.持久化消费进度到broker或者本地
                     * 2.移除本地队列
                     */
                    if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                        // 不归该消费者消费的队列，需要从 processQueueTable 移除
                        it.remove(); // 从 映射表 中移除
                        // 说明当前消费者消费的队列发生了变化
                        changed = true;
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }
                }

                // 如果当前遍历的 mq 还是被当前 消费者消费
                else if (pq.isPullExpired() /* 如果当前遍历的队列还归属当前消费者，则继续判断拉消息请求是否过期 */) {
                    // 如果2分钟内还没有发生拉消息的请求，则说明拉消息请求过期，可能是出问题了，会进入该分支中

                    // TODO 但是这个 队列 还是属于当前消费者呢，移除了之后是不是就没消费者消费该队列的消息了？？？
                    // 答：下看的循环就知道了，会给这些队列重新创建一个 ProcessQueue 重新塞入映射表中！！！！

                    switch (this.consumeType()) {
                        case CONSUME_ACTIVELY: // pull
                            break;
                        case CONSUME_PASSIVELY:// push
                            // push 的时候也会删除这样的队列

                            pq.setDropped(true);
                            /*
                             * 1.持久化消费进度到broker或者本地
                             * 2.移除本地队列
                             */
                            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                                it.remove();
                                changed = true;
                                // 一般到这里需要重启了
                                log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it", consumerGroup, mq);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        // 下面的逻辑是有新mq 分配来的场景

        // 拉消息的请求
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();

        // 遍历 最新分配给消费者的当前主题的队列集合
        for (MessageQueue mq : newMqSet /* 最新分配给消费者的当前主题的队列集合 */) {

            // 老的队列中不包含新的 mq,则说明该mq是新分配过来的
            if (!this.processQueueTable/* 此时的映射表中包含的队列是哪些呢？答：本次负载均衡之前就属于该消费者并且拉消息没有超时的队列 */.containsKey(mq/*则说明该mq是新分配过来的*/)) {
                if (isOrder && !this.lock(mq) /*获取队列的分布式锁*/) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    continue;
                }

                // 获取分布式锁成功

                // 先把可能存在的冗余(脏)数据删除掉
                this.removeDirtyOffset(mq);

                // 需要创建一个新的快照
                ProcessQueue pq = new ProcessQueue();

                // 计数从那个开始消费偏移量
                // 从 服务器 拉起该 mq 最新的消费进度
                long nextOffset = this.computePullFromWhere(mq);
                if (nextOffset >= 0) {
                    // 添加到映射表
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        // 不会进来这个分支！！！！

                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        // 创建请求
                        // 拉消息服务依赖 PullRequest 对象进行拉消息的工作，新分配的队列要创建这样的对象，最终放入拉消息服务本地主要是队列

                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        // 为新创建的队列创建 PullRequest 对象
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }
        }

        /**
         * 提交到拉消息服务
         * @see PullMessageService#executePullRequestImmediately(org.apache.rocketmq.client.impl.consumer.PullRequest)
         */
        this.dispatchPullRequest(pullRequestList);

        return changed;
    }

    /**
     * @param topic 主题
     * @param mqAll 该主题的所有队列
     * @param mqDivided 该主题在该消费者实例上新分配到的队列
     */
    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll, final Set<MessageQueue> mqDivided);

    /**
     * 1.持久化消费进度到broker或者本地
     * 2.移除本地队列
     */
    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean dropped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, dropped);
        }
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        for (Entry<MessageQueue, ProcessQueue> next : this.processQueueTable.entrySet()) {
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
    }
}

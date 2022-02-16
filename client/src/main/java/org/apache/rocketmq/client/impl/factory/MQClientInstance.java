package org.apache.rocketmq.client.impl.factory;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.RebalanceService;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.MQProducerInner;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;

import java.io.UnsupportedEncodingException;
import java.net.DatagramSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 一个jvm进程只有一个该类的实例！！！当然它下面的成员变量也一样
 *
 * 生产者跟消费者都用这个对象
 *
 * https://blog.csdn.net/prestigeding/article/details/91476328
 *
 * @see MQClientInstance#sendHeartbeatToAllBroker() 比较重要
 * @see MQClientInstance#persistAllConsumerOffset() 定时持久化，所以消息是可能重复消费的！！！
 */
@SuppressWarnings("all")
public class MQClientInstance {

    private final static long LOCK_TIMEOUT_MILLIS = 3000;

    private final InternalLogger log = ClientLogger.getLog();

    /**
     * @see org.apache.rocketmq.client.producer.DefaultMQProducer 其实就是 这个，因为 DefaultMQProducer 继承了 ClientConfig
     */
    @Getter
    private final ClientConfig clientConfig;

    /**
     * 客户端Id：ip&pid
     *
     * 一个 JVM 进程中，只有一个 MQClientInstance 实例，也就是说该 clientId 也是唯一的，
     * 但是，一个 JVM 进程中可以存在多个组，也可以存在多个消费者{@link MQConsumerInner}跟生产者{@link MQProducerInner}
     *
     * @see MQClientInstance#producerTable 该进程中的生产者列表
     * @see MQClientInstance#consumerTable 该进程中的消费者列表
     * @see ClientConfig#buildMQClientId() clientId 生成逻辑
     */
    @Getter
    private final String clientId;

    // 客户端启动时间
    @Getter
    private final long bootTimestamp = System.currentTimeMillis();

    /**
     * 客户端本地的路由数据，如果本地找不到某个主题的路由数据，则需要去namesrv拉。
     *
     * 当前实例的生产者跟消费者相关的主题的路数据都在这里！！
     *
     * 通过定时任务{@link MQClientInstance#startScheduledTask()}去 namesrv 拉取
     *
     * @see MQClientInstance#updateTopicRouteInfoFromNameServer()
     */
    @Getter
    private final ConcurrentMap<String/* Topic */, TopicRouteData/*主题路由信息*/> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();

    /**
     * TODO 生产者或者消费者是如何知道有哪些 broker 的呢？答：定时到 namesrv 上去拉取路由信息{@link MQClientInstance#updateTopicRouteInfoFromNameServer(java.lang.String, boolean, org.apache.rocketmq.client.producer.DefaultMQProducer)}
     * broker 物理节点映射表
     * key:brokerName 逻辑层面的对象
     * value:k是broker,0 的节点是主节点，其他是slave，v 是节点的地址 ip + port
     *
     * 这个数据其实就是从路由数据中解析出来的{@link MQClientInstance#topicRouteTable}
     */
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address：ip:port */>> brokerAddrTable = new ConcurrentHashMap<String, HashMap<Long, String>>();

    /**
     * broker 物理节点版本映射表
     * key:brokerName 逻辑层面的对象
     * value:k是broker,0 的节点是主节点，其他是slave，v 版本号
     *
     * versionMap.put(brokerAddr, version);
     */
    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address:ip:port */, Integer/* 心跳接口返回的版本号*/>> brokerVersionTable = new ConcurrentHashMap<String, HashMap<String, Integer>>();
    /**
     * 每个组下面有多个生产者或者发送者
     * 是很正常的事情
     */

    /**
     * 生产者映射表，key:组名称，value：生产者或者消费者
     *
     * @see MQClientInstance#registerProducer(java.lang.String, org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl)
     */
    private final ConcurrentMap<String/* group */, MQProducerInner/*DefaultMQProducerImpl*/> producerTable = new ConcurrentHashMap<String, MQProducerInner>();

    // 消费者映射表
    // 生产者映射表，key:组名称，value：生产者或者消费者
    private final ConcurrentMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<String, MQConsumerInner>();

    // 管理
    private final ConcurrentMap<String/* group */, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<String, MQAdminExtInner>();

    // 客户端网络层的配置
    @Getter
    private final NettyClientConfig nettyClientConfig;

    /**
     * 最底层的api
     * 核心api实现，它几乎包含了所有服务端api，他的作用是 将 mq 业务层的数据转换为网络层 RemotingCommand 对象
     * 然后使用内部的 nettyRemotingClient 网络层对象的 invoke 系列方法完成网络 IO
     */
    @Getter
    private final MQClientAPIImpl mQClientAPIImpl;

    // 不关心
    @Getter
    private final MQAdminImpl mQAdminImpl;

    private final Lock lockNamesrv = new ReentrantLock();

    private final Lock lockHeartbeat = new ReentrantLock();

    // 单线程的调度线程池，用于执行定时任务
    @Getter
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQClientFactoryScheduledThread");
        }
    });

    /**
     * 拉消息服务,与消费有关
     */
    @Getter
    private final PullMessageService pullMessageService;

    /**
     * 负载均衡服务,与消费有关
     */
    private final RebalanceService rebalanceService;

    /**
     * TODO 内部生产者，用于处理消费端消息回退 ? 是这样吗？
     *
     * CLIENT_INNER_PRODUCER
     *
     * this.defaultMQProducer = new DefaultMQProducer(CLIENT_INNER_PRODUCER);
     *
     * @see ConsumeMessageOrderlyService#sendMessageBack(org.apache.rocketmq.common.message.MessageExt)
     */
    @Getter
    private final DefaultMQProducer defaultMQProducer;

    /**
     * 统计相关的
     */
    @Getter
    private final ConsumerStatsManager consumerStatsManager;

    /**
     * 心跳次数的统计
     *
     * @see MQClientInstance#sendHeartbeatToAllBroker() 心跳
     */
    private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);

    // 客户端状态
    private ServiceState serviceState = ServiceState.CREATE_JUST;

    private DatagramSocket datagramSocket;

    private final Random random = new Random();

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        this.clientConfig = clientConfig;
        // 索引值一般是0，因为客户端实例一般都是一个进程只有一个
        this.nettyClientConfig = new NettyClientConfig();

        // clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
        int clientCallbackExecutorThreads = clientConfig.getClientCallbackExecutorThreads(); // Java虚拟机的可用的处理器数量
        this.nettyClientConfig.setClientCallbackExecutorThreads(clientCallbackExecutorThreads);
        this.nettyClientConfig.setUseTLS(clientConfig.isUseTLS());

        /**
         * 创建客户端协议处理器
         * 客户端协议处理器，用于处理IO事件
         * @see ClientRemotingProcessor#processRequest(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)
         */
        ClientRemotingProcessor clientRemotingProcessor = new ClientRemotingProcessor(this);

        // 创建 mQClientAPIImpl
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, clientRemotingProcessor/*客户端协议处理器，注册到客户端网络层*/, rpcHook, clientConfig);

        // 配置的 namesrv
        String configNamesrvAddr = this.clientConfig.getNamesrvAddr();
        if (configNamesrvAddr != null) {
            /**
             * 关系到生产者本地映射表
             *
             * @see NettyRemotingClient#namesrvAddrList 其实就是缓存到这
             */
            this.mQClientAPIImpl.updateNameServerAddressList(configNamesrvAddr);
            log.info("user specified name server address: {}", configNamesrvAddr);
        }
        this.clientId = clientId;
        this.mQAdminImpl = new MQAdminImpl(this);
        this.pullMessageService = new PullMessageService(this);
        this.rebalanceService = new RebalanceService(this);
        // 创建了一个 内部生产者实例，来完消息回退
        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP/*CLIENT_INNER_PRODUCER*/);
        // 复制配置
        this.defaultMQProducer.resetClientConfig(clientConfig);
        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);
    }

    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route/*主题路由信息*/) {
        TopicPublishInfo info = new TopicPublishInfo();
        info.setTopicRouteData(route);

        if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) {
            String[] brokers = route.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    MessageQueue mq = new MessageQueue(topic, item[0], i);
                    info.getMessageQueueList().add(mq);
                }
            }

            // 顺序消息
            info.setOrderTopic(true);
        } else {

            // 非顺序消息

            // 队列信息
            List<QueueData> queueDataList = route.getQueueDatas();
            Collections.sort(queueDataList);
            for (QueueData qd : queueDataList/*遍历该主题的所有队列*/) {
                if (PermName.isWriteable(qd.getPerm())/*当前队列是可写的*/) {
                    // 可写的队列

                    // 下面是试图找到当前 队列 对应的 brokerData 数据
                    BrokerData brokerData = null;

                    // 该主题所在的所有 broker 信息列表
                    List<BrokerData> brokerDataList = route.getBrokerDatas();
                    for (BrokerData bd : brokerDataList) {
                        if (bd.getBrokerName().equals(qd.getBrokerName())) {
                            brokerData = bd;
                            break;
                        }
                    }

                    if (null == brokerData) {
                        // 没有找到 broker 数据
                        continue;
                    }

                    if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)/*找到了 broker 节点数据，但是该 broker 没有 master 节点*/) {
                        // 如果找到的 broker 节点不包含主节点，则继续下次循环
                        continue;
                    }

                    // 在保证有主节点的情况下，根据写队列数量创建 消息队列

                    // 找到了主节点
                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {

                        // 创建队列
                        MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                        info.getMessageQueueList().add(mq);
                    }
                }
            }

            info.setOrderTopic(false);
        }

        return info;
    }

    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        Set<MessageQueue> mqList = new HashSet<MessageQueue>();
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {
            if (PermName.isReadable(qd.getPerm())) {
                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    mqList.add(mq);
                }
            }
        }
        return mqList;
    }

    /**
     * 生产者实例在启动的时候会调用该方法，一个JVM中可能会有多个生产者实例，也就是该方法会被调用多次，所以通过{@link MQClientInstance#serviceState}状态字段来控制真实的只启动一次
     *
     * @throws MQClientException
     * @see DefaultMQProducerImpl#start(boolean) 生产者实例在启动的时候会调用该方法
     */
    public void start() throws MQClientException {
        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    // 设置客户端实例的状态，启动成功后会修改
                    this.serviceState = ServiceState.START_FAILED;
                    // If not specified,looking address from name server
                    if (null == this.clientConfig.getNamesrvAddr()) {
                        // 一般不为空
                        this.mQClientAPIImpl.fetchNameServerAddr();
                    }
                    // Start request-response channel
                    // 启动客户端网络层入口
                    this.mQClientAPIImpl.start();
                    // Start various schedule tasks
                    this.startScheduledTask();
                    // Start pull service
                    this.pullMessageService.start();
                    // Start rebalance service
                    this.rebalanceService.start();
                    // Start push service

                    // 启动内部生产者，消息回退使用这个生产者
                    // this.defaultMQProducer = new DefaultMQProducer(CLIENT_INNER_PRODUCER);
                    DefaultMQProducerImpl defaultMQProducerImpl = this.defaultMQProducer.getDefaultMQProducerImpl();
                    defaultMQProducerImpl.start(false/*  用户的生产者传的 true ，内部生产者传 false，避免循环调用！！！*/);

                    log.info("the client factory [{}] start OK", this.clientId);

                    // 设置客户的状态为运行中
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

    /**
     * 1.定时获取 namesrv 地址
     * 2.定时从 namesrv 上更新客户端本地的路由数据
     * 3.定时清理下线的 broker 节点，定时发送心跳数据到每个 broker
     * 4.定时消费者持久化消费进度
     * 5.定时调整消费者线程池
     */
    private void startScheduledTask() {
        if (null == this.clientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                    } catch (Exception e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        // 定时任务1
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {

                    // 从 namesrv 上更新客户端本地的路由数据
                    MQClientInstance.this.updateTopicRouteInfoFromNameServer();
                } catch (Exception e) {
                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 10, this.clientConfig.getPollNameServerInterval()/*30秒*/, TimeUnit.MILLISECONDS);

        // 定时任务2
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    // 清理下线的 broker 节点
                    MQClientInstance.this.cleanOfflineBroker();
                    // 给在线的 broker 节点发送心跳数据
                    MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
                } catch (Exception e) {
                    log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                }
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval()/*30秒*/, TimeUnit.MILLISECONDS);

        // 定时任务3：消费者持久化消费进度
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    // 所以：可能重复消费
                    MQClientInstance.this.persistAllConsumerOffset();
                } catch (Exception e) {
                    log.error("ScheduledTask persistAllConsumerOffset exception", e);
                }
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval()/*5秒*/, TimeUnit.MILLISECONDS);

        // 定时任务4：调整消费者线程池
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.adjustThreadPool();
                } catch (Exception e) {
                    log.error("ScheduledTask adjustThreadPool exception", e);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    /**
     * 每隔三十秒去 namesrv 拉取每个主题的路由信息，并且缓存到生产者消费者本地
     */
    private void updateTopicRouteInfoFromNameServer() {

        // 客户端关心的主题的集合，分别从消费者跟生产者中提取
        Set<String> topicList = new HashSet<String>();

        // Consumer
        {
            for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    Set<SubscriptionData> subList = impl.subscriptions();
                    if (subList != null) {
                        for (SubscriptionData subData : subList) {
                            // 提取消费者关心的主题
                            String topic = subData.getTopic();
                            topicList.add(topic);
                        }
                    }
                }
            }
        }

        // Producer
        {
            for (Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    Set<String> lst = impl.getPublishTopicList();
                    // 提取生产者关心的主题
                    topicList.addAll(lst);
                }
            }
        }

        for (String topic : topicList) {
            // 遍历客户端主题集合，从namesrv拉去最新的主题路由数据，与本地客户端的路由数据对比，判断该主题是否需要更新
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    public Map<MessageQueue, Long> parseOffsetTableFromBroker(Map<MessageQueue, Long> offsetTable, String namespace) {
        HashMap<MessageQueue, Long> newOffsetTable = new HashMap<MessageQueue, Long>();
        if (StringUtils.isNotEmpty(namespace)) {
            for (Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
                MessageQueue queue = entry.getKey();
                queue.setTopic(NamespaceUtil.withoutNamespace(queue.getTopic(), namespace));
                newOffsetTable.put(queue, entry.getValue());
            }
        } else {
            newOffsetTable.putAll(offsetTable);
        }

        return newOffsetTable;
    }

    /**
     * Remove offline broker
     * <p>
     * 检查客户端路由表，将路由表中不包含的 addr 清理掉
     * 如果被清理的 brokerName 下所有的物理节点都没有了，需要将 broker 映射数据也清理掉
     */
    private void cleanOfflineBroker() {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    ConcurrentHashMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address：ip:port */>> updatedTable = new ConcurrentHashMap<String, HashMap<Long, String>>();
                    Iterator<Entry<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerTable.hasNext()) {
                        // 一个 brokerName 的主从节点地址信息
                        Entry<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> entry = itBrokerTable.next();
                        // 遍历每个 broker
                        String brokerName = entry.getKey();
                        // 该 broker 下的所有节点，brokerId - addr
                        HashMap<Long/* brokerId */, String/* address */> brokerAddrTable = entry.getValue();
                        HashMap<Long/* brokerId */, String/* address */> cloneAddrTable = new HashMap<Long, String>(brokerAddrTable);
                        Iterator<Entry<Long/* brokerId */, String/* address */>> it = cloneAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long/* brokerId */, String/* address */> ee = it.next();
                            // broker节点地址
                            String addr = ee.getValue();
                            // 判断该节点是否存在
                            if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
                                // 路由数据中已经没有这个broker地址的任何数据了，则说明该 broker 地址可以移除了
                                it.remove();
                                log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                            }
                        }

                        // 如果这个表都没移除了，则移除整个 broker
                        if (cloneAddrTable.isEmpty()) {
                            itBrokerTable.remove();
                            log.info("the broker[{}] name's host is offline, remove it", brokerName);
                        } else {
                            // 否则就更新
                            updatedTable.put(brokerName, cloneAddrTable);
                        }
                    }

                    if (!updatedTable.isEmpty()) {
                        this.brokerAddrTable.putAll(updatedTable);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.warn("cleanOfflineBroker Exception", e);
        }
    }

    /**
     * 主要是校验当前订阅的主题所在的 broker 是否支持 表达式类型
     * 因为不是每个 broker 都支持 除 TAG 之外的过滤类型的
     */
    public void checkClientInBroker() throws MQClientException {
        for (Entry<String/* group */, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            // 该生产者的订阅信息
            Set<SubscriptionData> subscriptionInner = entry.getValue().subscriptions();
            if (subscriptionInner == null || subscriptionInner.isEmpty()) {
                return;
            }

            for (SubscriptionData subscriptionData : subscriptionInner) {
                /**
                 *  public static final String SQL92 = "SQL92";
                 *  public static final String TAG = "TAG";
                 *  2 种类型
                 */
                String expressionType = subscriptionData.getExpressionType();
                if (ExpressionType.isTagType(expressionType)) {
                    continue;
                }
                // may need to check one broker every cluster...
                // assume that the configs of every broker in cluster are the the same.

                // 从客户端实例中拿到本地的路由信息，其中就有服务器地址
                String addr = findBrokerAddrByTopic(subscriptionData.getTopic());

                if (addr != null) {
                    try {
                        String consumerGroup = entry.getKey();
                        this.getMQClientAPIImpl().checkClientInBroker(addr, consumerGroup, this.clientId, subscriptionData, 3 * 1000);
                    } catch (Exception e) {
                        if (e instanceof MQClientException) {
                            throw (MQClientException) e;
                        } else {
                            String msg = "This error would not affect the launch of consumer, but may has impact on message receiving if you " + "have use the new features which are not supported by server, please check the log!";
                            throw new MQClientException("Check client in broker error, maybe because you use " + expressionType + " to filter message, but server has not been upgraded to support!" + msg, e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 订阅信息发送改变的时候会主动发心跳
     *
     * @see DefaultMQPushConsumerImpl#subscribe(java.lang.String, java.lang.String)
     */
    public void sendHeartbeatToAllBrokerWithLock() {
        if (this.lockHeartbeat.tryLock()) {
            try {
                this.sendHeartbeatToAllBroker();
                this.uploadFilterClassSource();
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed.");
        }
    }

    private void persistAllConsumerOffset() {
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            impl.persistConsumerOffset();
        }
    }

    public void adjustThreadPool() {
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (impl instanceof DefaultMQPushConsumerImpl) {
                        DefaultMQPushConsumerImpl dmq = (DefaultMQPushConsumerImpl) impl;
                        dmq.adjustThreadPool();
                    }
                } catch (Exception ignore) {
                }
            }
        }
    }

    //  遍历客户端主题集合，从namesrv拉去最新的主题路由数据，与本地客户端的路由数据对比，判断该主题是否需要更新
    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }

    private boolean isBrokerAddrExistInTopicRouteTable(final String brokerAddr) {
        // ConcurrentMap<String/* Topic */, TopicRouteData/*主题路由信息*/> topicRouteTable
        for (Entry<String/* Topic */, TopicRouteData/*主题路由信息*/> entry : this.topicRouteTable.entrySet()) {
            TopicRouteData topicRouteData/*主题路由信息*/ = entry.getValue();

            // 节点信息
            List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
            for (BrokerData bd : brokerDataList) {
                if (bd.getBrokerAddrs() != null) {
                    boolean exist = bd.getBrokerAddrs().containsValue(brokerAddr);
                    if (exist/*只要该broker地址存在就可以*/) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private void sendHeartbeatToAllBroker() {
        // 包括当前进程上的所有生产者，消费者信息，以及当前实例id,如：{"clientID":"10.201.13.28@9738","consumerDataSet":[],"producerDataSet":[{"groupName":"testProducerGroup"},{"groupName":"CLIENT_INNER_PRODUCER"}]}
        final HeartbeatData heartbeatData = this.prepareHeartbeatData();
        System.out.println("发送心跳的内容为：" + JSON.toJSONString(heartbeatData));
        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        if (producerEmpty && consumerEmpty) {
            log.warn("sending heartbeat, but no consumer and no producer");
            return;
        }
        // ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable
        if (this.brokerAddrTable.isEmpty()) {
            return;
        }
        // 用于打印日志
        long times = this.sendHeartbeatTimesTotal.getAndIncrement();
        // ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable
        /**
         * TODO 生产者或者消费者是如何知道有哪些 broker 的呢？答：定时到 namesrv 上去拉取路由信息{@link MQClientInstance#updateTopicRouteInfoFromNameServer(java.lang.String, boolean, org.apache.rocketmq.client.producer.DefaultMQProducer)}
         */
        for (Entry<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> entry : this.brokerAddrTable.entrySet()) {
            // 遍历每个 broker，并且发送心跳数据到 当前 brokerName 的 brokerId = 0 的 master 节点
            String brokerName = entry.getKey();
            // 该 broker 下面的所有的 节点地址映射 HashMap<Long/* brokerId */, String/* address */>
            HashMap<Long/* brokerId */, String/* address */> brokerAddrTable = entry.getValue();
            if (brokerAddrTable == null) {
                continue;
            }
            for (Entry<Long/* brokerId */, String/* address */> brokerIdAddress : brokerAddrTable.entrySet()) {
                // brokerId
                Long brokerId = brokerIdAddress.getKey();
                // 地址
                String brokerAddr = brokerIdAddress.getValue();
                if (brokerAddr == null) {
                    continue;
                }
                if (consumerEmpty/*TODO 为何在消费者为空的情况下呢？？？*/) {
                    if (brokerId != MixAll.MASTER_ID) {
                        // 不是主节点，则跳过
                        continue;
                    }
                }
                try {
                    // 发送心跳，心跳包中包含了很多信息： 包括所有生产者，消费者信息，以及当前实例id
                    int version = this.mQClientAPIImpl.sendHearbeat(brokerAddr/*发送心跳到每个master节点*/, heartbeatData, 3000);
                    if (!this.brokerVersionTable.containsKey(brokerName)) {
                        this.brokerVersionTable.put(brokerName, new HashMap<String, Integer>(4));
                    }
                    HashMap<String, Integer> versionMap = this.brokerVersionTable.get(brokerName);
                    versionMap.put(brokerAddr, version);
                    if (times % 20 == 0) {
                        log.info("send heart beat to broker[{} {} {}] success", brokerName, brokerId, brokerAddr);
                        log.info(heartbeatData.toString());
                    }
                } catch (Exception e) {
                    if (this.isBrokerInNameServer(brokerAddr)) {
                        log.info("send heart beat to broker[{} {} {}] failed", brokerName, brokerId, brokerAddr, e);
                    } else {
                        log.info("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName, brokerId, brokerAddr, e);
                    }
                }
            }
        }
    }

    private void uploadFilterClassSource() {
        for (Entry<String, MQConsumerInner> next : this.consumerTable.entrySet()) {
            MQConsumerInner consumer = next.getValue();
            if (ConsumeType.CONSUME_PASSIVELY == consumer.consumeType()) {
                Set<SubscriptionData> subscriptions = consumer.subscriptions();
                for (SubscriptionData sub : subscriptions) {
                    if (sub.isClassFilterMode() && sub.getFilterClassSource() != null) {
                        final String consumerGroup = consumer.groupName();
                        final String className = sub.getSubString();
                        final String topic = sub.getTopic();
                        final String filterClassSource = sub.getFilterClassSource();
                        try {
                            this.uploadFilterClassToAllFilterServer(consumerGroup, className, topic, filterClassSource);
                        } catch (Exception e) {
                            log.error("uploadFilterClassToAllFilterServer Exception", e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 生产者发送消息的时候会调用该方法去 nameServer 上拿到该 主题的路由信息
     *
     * 遍历客户端主题集合，从namesrv拉去最新的主题路由数据，与本地客户端的路由数据对比，判断该主题是否需要更新
     *
     * @param topic 主题
     * @param isDefault 是否默认
     * @param defaultMQProducer 发送方
     * @return 成功失败
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault/*定时任务 false， true*/, DefaultMQProducer defaultMQProducer/*定时任务 null 或者 defaultProducer*/) {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;
                    if (isDefault && defaultMQProducer != null) {
                        // TBW102
                        String createTopicKey = defaultMQProducer.getCreateTopicKey();
                        int defaultTopicQueueNums = defaultMQProducer.getDefaultTopicQueueNums();
                        // 使用模版信息
                        topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(createTopicKey/*TBW102*/, 1000 * 3);
                        if (topicRouteData != null) {
                            List<QueueData> queueDataList = topicRouteData.getQueueDatas();
                            for (QueueData data : queueDataList) {
                                int queueNums = Math.min(defaultTopicQueueNums, data.getReadQueueNums());
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);
                            }
                        }
                    } else {
                        // 到 namesrv 上拉去最新的主题路由数据
                        topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
                    }

                    if (topicRouteData != null) {
                        // 获取当前客户端本地的路由数据
                        // this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
                        // old 一般情况下是一个空对象，里面的字段都是默认值的那种对象
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        // 判断路由数据是否发送变化（本地跟远程的差异）
                        boolean changed = topicRouteDataIsChange(old, topicRouteData);
                        if (!changed) {

                            // 如果上面没发生变化，则继续判断是否需要更新
                            // 从每个生产者跟消费者中逐一判断
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } else {
                            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }
                        if (changed) {
                            // 克隆
                            // 到 namesrv 上拉去最新的主题路由数据
                            TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();

                            // 订阅该主题的 broker 节点集合
                            List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
                            for (BrokerData bd : brokerDataList) {
                                HashMap<Long/* brokerId,如 0为master，其他为 slave节点  */, String/* broker address 如 127.0.0.1:10911*/> brokerAddrs = bd.getBrokerAddrs();
                                this.brokerAddrTable.put(bd.getBrokerName(), brokerAddrs);
                            }

                            // Update Pub info 发布
                            {
                                // 将当前topic路由数据转化为主题发布数据
                                TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                publishInfo.setHaveTopicRouterInfo(true);
                                for (Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
                                    MQProducerInner impl = entry.getValue();
                                    if (impl != null) {
                                        // 生产者将主题发布数据保存到它本地。方便发送消息的时候使用
                                        impl.updateTopicPublishInfo(topic, publishInfo);
                                    }
                                }
                            }

                            // Update sub info 订阅
                            {
                                Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
                                    MQConsumerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                    }
                                }
                            }
                            log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    } else {
                        log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
                    }
                } catch (MQClientException e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.equals(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
                        log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                    }
                } catch (RemotingException e) {
                    log.error("updateTopicRouteInfoFromNameServer Exception", e);
                    throw new IllegalStateException(e);
                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }

        return false;
    }

    private HeartbeatData prepareHeartbeatData() {
        HeartbeatData heartbeatData = new HeartbeatData();
        // clientID
        heartbeatData.setClientID(this.clientId);

        // 当前进程上的所有消费者
        // ConcurrentMap<String/* group */, MQConsumerInner> consumerTable
        for (Map.Entry<String /* group */, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            // 消费者
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.groupName());
                consumerData.setConsumeType(impl.consumeType());
                consumerData.setMessageModel(impl.messageModel());
                consumerData.setConsumeFromWhere(impl.consumeFromWhere());
                consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                consumerData.setUnitMode(impl.isUnitMode());
                // 把该消费者信息添加到集合
                heartbeatData.getConsumerDataSet().add(consumerData);
            }
        }

        // 当前进程上的所有生产者
        // ConcurrentMap<String/* group */, MQProducerInner>
        for (Map.Entry<String/* group */, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                ProducerData producerData = new ProducerData();
                String groupName = entry.getKey();
                producerData.setGroupName(groupName);
                // 把该生产者信息添加到集合
                heartbeatData.getProducerDataSet().add(producerData);
            }
        }

        return heartbeatData;
    }

    private boolean isBrokerInNameServer(final String brokerAddr) {
        for (Entry<String, TopicRouteData> itNext : this.topicRouteTable.entrySet()) {
            List<BrokerData> brokerDatas = itNext.getValue().getBrokerDatas();
            for (BrokerData bd : brokerDatas) {
                boolean contain = bd.getBrokerAddrs().containsValue(brokerAddr);
                if (contain) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * This method will be removed in the version 5.0.0,because filterServer was removed,and method
     * <code>subscribe(final String topic, final MessageSelector messageSelector)</code> is recommended.
     */
    @Deprecated
    private void uploadFilterClassToAllFilterServer(final String consumerGroup, final String fullClassName, final String topic, final String filterClassSource) throws UnsupportedEncodingException {
        byte[] classBody = null;
        int classCRC = 0;
        try {
            classBody = filterClassSource.getBytes(MixAll.DEFAULT_CHARSET);
            classCRC = UtilAll.crc32(classBody);
        } catch (Exception e1) {
            log.warn("uploadFilterClassToAllFilterServer Exception, ClassName: {} {}", fullClassName, RemotingHelper.exceptionSimpleDesc(e1));
        }

        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null && topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            for (Entry<String, List<String>> next : topicRouteData.getFilterServerTable().entrySet()) {
                List<String> value = next.getValue();
                for (final String fsAddr : value) {
                    try {
                        this.mQClientAPIImpl.registerMessageFilterClass(fsAddr, consumerGroup, topic, fullClassName, classCRC, classBody, 5000);

                        log.info("register message class filter to {} OK, ConsumerGroup: {} Topic: {} ClassName: {}", fsAddr, consumerGroup, topic, fullClassName);

                    } catch (Exception e) {
                        log.error("uploadFilterClassToAllFilterServer Exception", e);
                    }
                }
            }
        } else {
            log.warn("register message class filter failed, because no filter server, ConsumerGroup: {} Topic: {} ClassName: {}", consumerGroup, topic, fullClassName);
        }
    }

    private boolean topicRouteDataIsChange(TopicRouteData olddata/*old一般情况下是一个空对象，里面的字段都是默认值的那种对象*/, TopicRouteData nowdata/*刚从namesrv获取的*/) {
        if (olddata == null || nowdata == null) {
            return true;
        }
        // old一般情况下是一个空对象，里面的字段都是默认值的那种对象
        TopicRouteData old = olddata.cloneTopicRouteData();
        // 刚从namesrv获取的
        TopicRouteData now = nowdata.cloneTopicRouteData();
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);

    }

    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        boolean result = false;
        {
            // ConcurrentMap<String/* group */, MQProducerInner/*DefaultMQProducerImpl*/> producerTable
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isPublishTopicNeedUpdate(topic);
                }
            }
        }

        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isSubscribeTopicNeedUpdate(topic);
                }
            }
        }

        return result;
    }

    /**
     * 如下，某个生产者实例或者消费者实例在关闭的时候就会调用全局唯一实例 MQClientInstance 的关闭方法，当然，只有注册在当前实例的所有生产者或者消费者都关闭的时候才能真正的关闭当前实例
     *
     * @see DefaultMQProducerImpl#shutdown(boolean)
     * @see DefaultMQPushConsumerImpl#shutdown()
     */
    public void shutdown() {
        // Consumer
        if (!this.consumerTable.isEmpty()) {
            // 某个生产者实例或者消费者实例在关闭的时候就会调用全局唯一实例 MQClientInstance 的关闭方法，当然，只有注册在当前实例的所有生产者或者消费者都关闭的时候才能真正的关闭当前实例
            return;
        }

        // AdminExt
        if (!this.adminExtTable.isEmpty()) {
            // 某个生产者实例或者消费者实例在关闭的时候就会调用全局唯一实例 MQClientInstance 的关闭方法，当然，只有注册在当前实例的所有生产者或者消费者都关闭的时候才能真正的关闭当前实例
            return;
        }

        // Producer
        if (this.producerTable.size() > 1) {
            // 某个生产者实例或者消费者实例在关闭的时候就会调用全局唯一实例 MQClientInstance 的关闭方法，当然，只有注册在当前实例的所有生产者或者消费者都关闭的时候才能真正的关闭当前实例
            return;
        }

        // 到这里，当前关闭的是最后一个实例

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    break;
                case RUNNING:
                    DefaultMQProducerImpl defaultMQProducerImpl = this.defaultMQProducer.getDefaultMQProducerImpl();
                    defaultMQProducerImpl.shutdown(false);

                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    // 拉消息
                    this.pullMessageService.shutdown(true);
                    // 异常消息
                    this.scheduledExecutorService.shutdown();
                    // 网络层
                    this.mQClientAPIImpl.shutdown();
                    // 消费者重平衡
                    this.rebalanceService.shutdown();

                    if (this.datagramSocket != null) {
                        this.datagramSocket.close();
                        this.datagramSocket = null;
                    }

                    // 移除当前实例
                    MQClientManager.getInstance().removeClientFactory(this.clientId);
                    log.info("the client factory [{}] shutdown OK", this.clientId);
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }
    }

    public boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }

        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            log.warn("the consumer group[" + group + "] exist already.");
            return false;
        }

        return true;
    }

    public void unregisterConsumer(final String group) {
        this.consumerTable.remove(group);
        this.unregisterClientWithLock(null, group);
    }

    private void unregisterClientWithLock(final String producerGroup, final String consumerGroup) {
        try {
            if (this.lockHeartbeat.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    this.unregisterClient(producerGroup, consumerGroup);
                } catch (Exception e) {
                    log.error("unregisterClient exception", e);
                } finally {
                    this.lockHeartbeat.unlock();
                }
            } else {
                log.warn("lock heartBeat, but failed.");
            }
        } catch (InterruptedException e) {
            log.warn("unregisterClientWithLock exception", e);
        }
    }

    /**
     * 关闭某个生产者或者消费者的时候会调用该接口
     *
     * @param producerGroup 生产者组
     * @param consumerGroup 消费者组
     */
    private void unregisterClient(final String producerGroup, final String consumerGroup) {

        // ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address：ip:port */>> brokerAddrTable
        for (Entry<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address：ip:port */>> entry : this.brokerAddrTable.entrySet()) {
            String brokerName = entry.getKey();
            HashMap<Long/* brokerId */, String/* address：ip:port */> brokerAddrTable = entry.getValue();
            if (brokerAddrTable == null) {
                continue;
            }
            for (Entry<Long/* brokerId */, String/* address：ip:port */> addressEntry : brokerAddrTable.entrySet()) {
                String addr/* address：ip:port */ = addressEntry.getValue();
                if (addr == null) {
                    continue;
                }
                try {
                    this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, 3000);
                } catch (RemotingException e) {
                    log.error("unregister client exception from broker: " + addr, e);
                } catch (InterruptedException e) {
                    log.error("unregister client exception from broker: " + addr, e);
                } catch (MQBrokerException e) {
                    log.error("unregister client exception from broker: " + addr, e);
                }
            }
        }
    }

    /**
     * 每个生产者实例启动{@link DefaultMQProducerImpl#start(boolean)} 的时候都会把自己注册到进程唯一的 MQClientInstance 实例上
     *
     * @param group 生产者组
     * @param producer 生产者实例
     * @return 成功失败
     */
    public boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }

        // ConcurrentMap<String/* group */, MQProducerInner/*DefaultMQProducerImpl*/> producerTable = new ConcurrentHashMap<String, MQProducerInner>();
        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            // 说明之前就已经注册过了，不能重复注册
            return false;
        }
        return true;
    }

    public void unregisterProducer(final String group) {
        // ConcurrentMap<String/* group */, MQProducerInner/*DefaultMQProducerImpl*/> producerTable
        // 从本地客户端实例中移除！
        this.producerTable.remove(group);
        this.unregisterClientWithLock(group, null);
    }

    public boolean registerAdminExt(final String group, final MQAdminExtInner admin) {
        if (null == group || null == admin) {
            return false;
        }

        MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
        if (prev != null) {
            log.warn("the admin group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterAdminExt(final String group) {
        this.adminExtTable.remove(group);
    }

    public void rebalanceImmediately() {
        this.rebalanceService.wakeup();
    }

    public void doRebalance() {
        for (Map.Entry<String/* group */, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    // 调用每个消费者实例的重平衡方法
                    impl.doRebalance();
                } catch (Throwable e) {
                    log.error("doRebalance exception", e);
                }
            }
        }
    }

    public MQProducerInner selectProducer(final String group) {
        return this.producerTable.get(group);
    }

    public MQConsumerInner selectConsumer(final String group) {
        return this.consumerTable.get(group);
    }

    public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        // 当前 brokerName 部署的所有节点地址映射表
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            for (Map.Entry<Long/* brokerId */, String/* address */> entry : map.entrySet()) {
                Long id = entry.getKey();
                brokerAddr = entry.getValue();
                if (brokerAddr != null) {
                    found = true;
                    if (MixAll.MASTER_ID == id) {
                        slave = false;
                    } else {
                        slave = true;
                    }
                    // 找到第一个即可
                    break;
                }
            } // end of for
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public String findBrokerAddressInPublish(final String brokerName) {
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }

    public FindBrokerResult findBrokerAddressInSubscribe(final String brokerName, final long brokerId, final boolean onlyThisBroker) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        // 获取该 brokerName 的地址分布映射表
        HashMap<Long/* brokerId */, String/* address：ip:port  */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            brokerAddr = map.get(brokerId);
            slave = brokerId != MixAll.MASTER_ID;
            found = brokerAddr != null;

            if (!found && !onlyThisBroker) {
                Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                slave = entry.getKey() != MixAll.MASTER_ID;
                found = true;
            }
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public int findBrokerVersion(String brokerName, String brokerAddr) {
        if (this.brokerVersionTable.containsKey(brokerName)) {
            if (this.brokerVersionTable.get(brokerName).containsKey(brokerAddr)) {
                return this.brokerVersionTable.get(brokerName).get(brokerAddr);
            }
        }
        //To do need to fresh the version
        return 0;
    }

    public List<String> findConsumerIdList(final String topic, final String group) {
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            this.updateTopicRouteInfoFromNameServer(topic);
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }

        if (null != brokerAddr) {
            try {
                return this.mQClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, 3000);
            } catch (Exception e) {
                log.warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + group, e);
            }
        }

        return null;
    }

    public String findBrokerAddrByTopic(final String topic) {
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null) {
            List<BrokerData> brokers = topicRouteData.getBrokerDatas();
            if (!brokers.isEmpty()) {
                int index = random.nextInt(brokers.size());
                BrokerData bd = brokers.get(index % brokers.size());
                return bd.selectBrokerAddr();
            }
        }

        return null;
    }

    public void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
        DefaultMQPushConsumerImpl consumer = null;
        try {
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
                consumer = (DefaultMQPushConsumerImpl) impl;
            } else {
                log.info("[reset-offset] consumer dose not exist. group={}", group);
                return;
            }
            consumer.suspend();

            ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
            for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
                MessageQueue mq = entry.getKey();
                if (topic.equals(mq.getTopic()) && offsetTable.containsKey(mq)) {
                    ProcessQueue pq = entry.getValue();
                    pq.setDropped(true);
                    pq.clear();
                }
            }

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException ignore) {
            }

            Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
            while (iterator.hasNext()) {
                MessageQueue mq = iterator.next();
                Long offset = offsetTable.get(mq);
                if (topic.equals(mq.getTopic()) && offset != null) {
                    try {
                        consumer.updateConsumeOffset(mq, offset);
                        consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, processQueueTable.get(mq));
                        iterator.remove();
                    } catch (Exception e) {
                        log.warn("reset offset failed. group={}, {}", group, mq, e);
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.resume();
            }
        }
    }

    public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
        MQConsumerInner impl = this.consumerTable.get(group);
        if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else if (impl != null && impl instanceof DefaultMQPullConsumerImpl) {
            DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else {
            return Collections.EMPTY_MAP;
        }
    }

    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String consumerGroup, final String brokerName) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (null != mqConsumerInner) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;

            ConsumeMessageDirectlyResult result = consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
            return result;
        }

        return null;
    }

    public ConsumerRunningInfo consumerRunningInfo(final String consumerGroup) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);

        ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();

        List<String> nsList = this.mQClientAPIImpl.getRemotingClient().getNameServerAddressList();

        StringBuilder strBuilder = new StringBuilder();
        if (nsList != null) {
            for (String addr : nsList) {
                strBuilder.append(addr).append(";");
            }
        }

        String nsAddr = strBuilder.toString();
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType().name());
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CLIENT_VERSION, MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));

        return consumerRunningInfo;
    }
}

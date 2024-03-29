package org.apache.rocketmq.client.impl.producer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.client.producer.RequestFutureTable;
import org.apache.rocketmq.client.producer.RequestResponseFuture;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.CorrelationIdUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("all")
public class DefaultMQProducerImpl implements MQProducerInner {

    private final InternalLogger log = ClientLogger.getLog();

    // 没有实际业务意义，打印日志的时候使用，生产invokeId
    private final Random random = new Random();

    /**
     * 就是前面的生产者门面对象,在这里主要当作{@link ClientConfig} config使用,没错他与当前类就是循环依赖！！！！！！
     */
    @Getter
    private final DefaultMQProducer defaultMQProducer;

    /**
     * 映射表
     * 主题发布信息映射表
     * 作为一个生产者，你肯定要知道你发送消息各个主题的路由信息
     */
    @Getter
    private final ConcurrentMap<String/*topic*/, TopicPublishInfo/*该主题的发布信息*/> topicPublishInfoTable = new ConcurrentHashMap<String, TopicPublishInfo>();

    // 发送消息的钩子，留给用户扩展，如：消息轨迹
    private final ArrayList<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();

    // 该对象最终传递给 NettyRemotingClient,留给用户扩展
    private final RPCHook rpcHook;

    /**
     * 缺省的异步发送消息线程池,执行异步发送任务
     *
     * @see DefaultMQProducerImpl#DefaultMQProducerImpl(org.apache.rocketmq.client.producer.DefaultMQProducer, org.apache.rocketmq.remoting.RPCHook)
     */
    private final ExecutorService defaultAsyncSenderExecutor;

    /**
     * 定时任务：执行 RequestFutureTable.scanExpireRequest()
     *
     * @see RequestFutureTable#requestFutureTable
     * @see RequestFutureTable#scanExpiredRequest()
     */
    private final Timer timer = new Timer("RequestHouseKeepingService", true);

    // 事务消息检查事务状态的任务
    protected BlockingQueue<Runnable> checkRequestQueue;

    /**
     * 事务相关，事务回查
     *
     * @see DefaultMQProducerImpl#checkTransactionState(java.lang.String, org.apache.rocketmq.common.message.MessageExt, org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader)
     */
    protected ExecutorService checkExecutor;

    /**
     * 当前生产者对象的状态
     */
    @Getter
    @Setter
    private ServiceState serviceState = ServiceState.CREATE_JUST;

    /**
     * 客户端实例对象，生产者启动后需要注册到该客户端对象中（观察者模式）
     * this.mQClientFactory = mqClientManager.getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);
     *
     * @see MQClientManager#getOrCreateMQClientInstance(org.apache.rocketmq.client.ClientConfig, org.apache.rocketmq.remoting.RPCHook)
     */
    @Getter
    private MQClientInstance mQClientFactory;

    // 钩子，扩展用，注意和 sendMessageHook 区别，它可以抛异常。控制消息是否可以发送
    private final ArrayList<CheckForbiddenHook> checkForbiddenHookList = new ArrayList<CheckForbiddenHook>();

    // zip 压缩级别，默认5
    @Getter
    @Setter
    private int zipCompressLevel = Integer.parseInt(System.getProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "5"));

    // 选择队列容错策略
    private final MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();

    /**
     * 异步发送线程池，如果指定的话，就不再使用默认{@link defaultAsyncSenderExecutor}的线程池，而是使用该线程池
     */
    private ExecutorService asyncSenderExecutor;

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer) {
        this(defaultMQProducer, null);
    }

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer, RPCHook rpcHook) {
        this.defaultMQProducer = defaultMQProducer;
        this.rpcHook = rpcHook;
        /**
         * 异步发送消息的时候使用到的队列
         */
        BlockingQueue<Runnable> asyncSenderThreadPoolQueue = new LinkedBlockingQueue<Runnable>(50000);
        // 创建缺省的异步任务线程池
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        // 默认异步消息发送线程池
        this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(availableProcessors/*核心线程数量*/, availableProcessors/*最大数量*/, 1000 * 60/*空闲线程的存活时间*/, TimeUnit.MILLISECONDS, asyncSenderThreadPoolQueue/*异步消息线程池任务队列*/,
                new ThreadFactory() { // 线程工厂
                    private AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncSenderExecutor_"/*线程前缀*/ + this.threadIndex.incrementAndGet());
                    }
                });
    }

    public void registerCheckForbiddenHook(CheckForbiddenHook checkForbiddenHook) {
        this.checkForbiddenHookList.add(checkForbiddenHook);
    }

    /**
     * @see TransactionMQProducer#start() 事务消息生产者
     */
    public void initTransactionEnv() {
        TransactionMQProducer producer = (TransactionMQProducer) this.defaultMQProducer;
        ExecutorService executorService = producer.getExecutorService();
        if (executorService != null) {
            this.checkExecutor = executorService;
        } else {
            this.checkRequestQueue = new LinkedBlockingQueue<Runnable>(producer.getCheckRequestHoldMax());
            this.checkExecutor = new ThreadPoolExecutor(producer.getCheckThreadPoolMinSize(), producer.getCheckThreadPoolMaxSize(), 1000 * 60, TimeUnit.MILLISECONDS, this.checkRequestQueue);
        }
    }

    public void destroyTransactionEnv() {
        if (this.checkExecutor != null) {
            this.checkExecutor.shutdown();
        }
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
    }

    public void start() throws MQClientException {
        this.start(true);
    }

    // 正常路径： startFactory 为 true
    public void start(final boolean startFactory) throws MQClientException {
        String producerGroup = this.defaultMQProducer.getProducerGroup();
        switch (this.serviceState) {
            case CREATE_JUST:// 刚刚创建的就是 CREAT_JUST 状态
                this.serviceState = ServiceState.START_FAILED;// 状态修改为启动失败，后面如果成功会修改的
                this.checkConfig();// 1.组名称不能空，也不能为  DEFAULT_PRODUCER
                if (!producerGroup.equals(MixAll.CLIENT_INNER_PRODUCER_GROUP/*CLIENT_INNER_PRODUCER*/)) {
                    // 条件成立：说明当前生产者不是 内部生产者（什么是内部生产者？处理消息回退这种情况使用的生产者，就是 CLIENT_INNER_PRODUCER 生产者组）
                    // 正常会进来这里
                    // 修改生产者实例名称为：当前进程的PID
                    //  this.instanceName = String.valueOf(pid);
                    /**
                     * @see ClientConfig#instanceName
                     */
                    this.defaultMQProducer.changeInstanceNameToPID();
                }
                // 获取当前进程的RocketMQ客户端实例对象，其中包括设置 clientId 的逻辑，一般为：10.201.13.28@9738
                MQClientManager mqClientManager = MQClientManager.getInstance();
                // 每个 client 都有一个不变的实例
                // ConcurrentMap<String/*clientId：IP&instanceName（当前进程PID），如：10.201.13.28@9738*/, MQClientInstance> factoryTable
                // 一个 JVM 进程只有一个实例！！！！
                this.mQClientFactory = mqClientManager.getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);
                /**
                 * 将生产者自己注册到mq客户端实例内(观察者模式)，
                 * 一个客户端实例可能会有多个生产者跟消费者哦
                 */
                boolean registerOK = mQClientFactory.registerProducer(producerGroup, this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    throw new MQClientException("The producer group[" + producerGroup + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
                }
                String createTopicKey/*TBW102*/ = this.defaultMQProducer.getCreateTopicKey();
                // 内置的主题信息放置到主题发布信息映射表中
                this.topicPublishInfoTable.put(createTopicKey/*TBW102*/, new TopicPublishInfo());
                if (startFactory/*正常启动传的 true*/) {
                    // 启动 RocketMq 客户端实例对象 入口
                    mQClientFactory.start();
                }
                // 设置生产者实例设置为运行中
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                // 重复调用
                throw new MQClientException("The producer service state not OK, maybe started once, " + this.serviceState + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
            default:
                break;
        }
        // 强制rocketMq客户端实例（生产者实例）向已知的broker节点发送心跳
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
        /**
         * request方法发送的消息需要 消费者回执一条消息
         * 怎么实现的呢？
         * 生产者msg加了一些信息，关联ID客户端ID，发送到broker之后，消费者从 broker 拿到这条消息，检查msg,发现是一个需要回执的消息
         * 处理完消息之后，根据msg关联的ID以及客户端Id生成一条消息（封装响应给生产者的结果）发送到broker。
         * broker拿到这条消息之后，它知道这是一条回执消息，根据客户端Id找到ch，将消息推送给生产者，
         * 生产者这边拿到回执消息之后，读取出来关联ID，找到对应的 RequestFuture，将阻塞的线程唤醒
         *
         * 类似 生产者 和 消费者 之间进行了一次 RPC，只不过 中间由 broker 代理完成！
         */
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    // 这个定时任务主要是处理，上面的回执过期的情况
                    RequestFutureTable.scanExpiredRequest();
                } catch (Throwable e) {
                    log.error("scan RequestFutureTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }

    private void checkConfig() throws MQClientException {
        String producerGroup = this.defaultMQProducer.getProducerGroup();
        Validators.checkGroup(producerGroup);

        if (null == producerGroup) {
            throw new MQClientException("producerGroup is null", null);
        }

        if (producerGroup.equals(MixAll.DEFAULT_PRODUCER_GROUP/*DEFAULT_PRODUCER*/)) {
            throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.", null);
        }
    }

    public void shutdown() {
        this.shutdown(true);
    }

    public void shutdown(final boolean shutdownFactory) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                String producerGroup = this.defaultMQProducer.getProducerGroup();
                /**
                 * 这里发请求
                 *
                 * @see org.apache.rocketmq.broker.processor.ClientManageProcessor#unregisterClient(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand) 这个方法处理
                 */
                this.mQClientFactory.unregisterProducer(producerGroup);
                // 关闭异步发送线程池
                this.defaultAsyncSenderExecutor.shutdown();
                if (shutdownFactory) {
                    this.mQClientFactory.shutdown();
                }

                log.info("the producer [{}] shutdown OK", producerGroup);
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    @Override
    public Set<String> getPublishTopicList() {
        Set<String> topicList = new HashSet<String>();
        // ConcurrentMap<String/*topic*/, TopicPublishInfo/*该主题的发布信息*/> topicPublishInfoTable = new ConcurrentHashMap<String, TopicPublishInfo>();
        for (String key/*topic*/ : this.topicPublishInfoTable.keySet()) {
            topicList.add(key);
        }

        return topicList;
    }

    @Override
    public boolean isPublishTopicNeedUpdate(String topic) {
        // ConcurrentMap<String/*topic*/, TopicPublishInfo/*该主题的发布信息*/>
        TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);

        return null == prev || !prev.ok();
    }

    /**
     * This method will be removed in the version 5.0.0 and <code>getCheckListener</code> is recommended.
     *
     * @return
     */
    @Override
    @Deprecated
    public TransactionCheckListener checkListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionCheckListener();
        }

        return null;
    }

    @Override
    public TransactionListener getCheckListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionListener();
        }
        return null;
    }

    @Override
    public void checkTransactionState(final String addr, final MessageExt msg, final CheckTransactionStateRequestHeader header) {
        // 创建一个任务
        Runnable request = new Runnable() {
            private final String brokerAddr = addr;

            private final MessageExt message = msg;

            private final CheckTransactionStateRequestHeader checkRequestHeader = header;

            private final String group = DefaultMQProducerImpl.this.defaultMQProducer.getProducerGroup();

            @Override
            public void run() {
                // 内部类的巧妙之处！
                // 老的
                TransactionCheckListener transactionCheckListener = DefaultMQProducerImpl.this.checkListener();
                // 新的
                TransactionListener transactionListener = getCheckListener();
                if (transactionCheckListener != null || transactionListener != null) {
                    LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
                    Throwable exception = null;
                    try {
                        if (transactionCheckListener != null) {
                            // 交给用户自己处理
                            localTransactionState = transactionCheckListener.checkLocalTransactionState(message);
                        } else if (transactionListener != null) {
                            log.debug("Used new check API in transaction message");
                            localTransactionState = transactionListener.checkLocalTransaction(message);
                        } else {
                            log.warn("CheckTransactionState, pick transactionListener by group[{}] failed", group);
                        }
                    } catch (Throwable e) {
                        log.error("Broker call checkTransactionState, but checkLocalTransactionState exception", e);
                        exception = e;
                    }
                    this.processTransactionState(localTransactionState, group, exception);
                } else {
                    // 没有配置事务回查监听器，怎么过
                    log.warn("CheckTransactionState, pick transactionCheckListener by group[{}] failed", group);
                }
            }

            private void processTransactionState(final LocalTransactionState localTransactionState/*事务消息回查结果*/, final String producerGroup, final Throwable exception/*回查过程异常*/) {
                final EndTransactionRequestHeader thisHeader = new EndTransactionRequestHeader();
                thisHeader.setCommitLogOffset(checkRequestHeader.getCommitLogOffset());
                thisHeader.setProducerGroup(producerGroup);
                thisHeader.setTranStateTableOffset(checkRequestHeader.getTranStateTableOffset());
                thisHeader.setFromTransactionCheck(true);
                String uniqueKey = message.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX/*UNIQ_KEY*/);
                if (uniqueKey == null) {
                    uniqueKey = message.getMsgId();
                }
                thisHeader.setMsgId(uniqueKey);
                thisHeader.setTransactionId(checkRequestHeader.getTransactionId());
                switch (localTransactionState) {
                    case COMMIT_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                        break;
                    case ROLLBACK_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                        log.warn("when broker check, client rollback this transaction, {}", thisHeader);
                        break;
                    case UNKNOW:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                        log.warn("when broker check, client does not know this transaction state, {}", thisHeader);
                        break;
                    default:
                        break;
                }
                String remark = null;
                if (exception != null) {
                    // 回查发生了异常
                    remark = "checkLocalTransactionState Exception: " + RemotingHelper.exceptionSimpleDesc(exception);
                }
                try {
                    // 事务回查之后，把回查结果返回给 broker，broker 根据该结果决定事务消息的进一步处理
                    MQClientAPIImpl mqClientAPIImpl = DefaultMQProducerImpl.this.mQClientFactory.getMQClientAPIImpl();
                    mqClientAPIImpl.endTransactionOneway(brokerAddr, thisHeader, remark, 3000);
                } catch (Exception e) {
                    log.error("endTransactionOneway exception", e);
                }
            }
        };

        // 提交任务
        this.checkExecutor.submit(request);
    }

    @Override
    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev.toString());
            }
        }
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQProducer.isUnitMode();
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.makeSureStateOK();
        Validators.checkTopic(newTopic);
        MQAdminImpl adminImpl = this.mQClientFactory.getMQAdminImpl();
        adminImpl.createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    private void makeSureStateOK() throws MQClientException {
        // 确认生产者状态是运行中
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The producer service state not OK, " + this.serviceState + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK), null);
        }
    }

    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    /**
     * DEFAULT ASYNC -------------------------------------------------------
     */
    public void send(Message msg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        int sendMsgTimeout = this.defaultMQProducer.getSendMsgTimeout();
        send(msg, sendCallback, sendMsgTimeout);
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param sendCallback
     * @param timeout the <code>sendCallback</code> will be invoked at most time
     * @throws RejectedExecutionException
     */
    @Deprecated
    public void send(final Message msg, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();

        // return null == asyncSenderExecutor ? defaultAsyncSenderExecutor : asyncSenderExecutor;
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            sendDefaultImpl(msg, CommunicationMode.ASYNC, sendCallback, timeout - costTime);
                        } catch (Exception e) {
                            sendCallback.onException(e);
                        }
                    } else {

                        // 超时异常
                        sendCallback.onException(new RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout"));
                    }
                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }

    }

    /**
     * @param tpInfo 主题发布信息
     * @param lastBrokerName 上次发送失败的 brokerName 如果是第一次发送则为null
     * @return 队列
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        return this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency/*发送消息的耗时*/, boolean isolation) {
        this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation);
    }

    private void validateNameServerSetting() throws MQClientException {
        List<String> nsList = this.getmQClientFactory().getMQClientAPIImpl().getNameServerAddressList();
        if (null == nsList || nsList.isEmpty()) {
            throw new MQClientException("No name server address, please set it." + FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL), null).setResponseCode(ClientErrorCode.NO_NAME_SERVER_EXCEPTION);
        }

    }

    private SendResult sendDefaultImpl(Message msg, final CommunicationMode communicationMode/*发送模式，同步，异步，单项*/, final SendCallback sendCallback/*回调，同步发送的时候这个参数是空，只有异步的时候才有值*/, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        // 确定生产者状态是运行中，否则抛出异常
        this.makeSureStateOK();
        // 校验，一般都会通过
        Validators.checkMessage(msg, this.defaultMQProducer);
        // 打印日志使用
        final long invokeID = random.nextLong();
        // 发送初试时间
        long beginTimestampFirst = System.currentTimeMillis();
        // 本轮发送开始时间,发送失败的时候会重试
        long beginTimestampPrev = beginTimestampFirst;
        // 本轮发送结束时间
        long endTimestamp = beginTimestampFirst;
        // 获取当前主题的发布信息，需要依赖它里面的 MessageQueues 信息选择队列后面去发送消息使用
        String topic = msg.getTopic();
        // 先试图本地查询，如果本地没有，则去 namesrv 上获取，需要依赖该对象里面的 messageQueue，后面发送消息的时候使用
        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(topic);
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            boolean callTimeout = false; // 是否超时
            MessageQueue mq = null; // 选中的发送消息的队列
            Exception exception = null; // 异常
            SendResult sendResult = null; // 发送结果

            // 总的发送次数
            int retryTimesWhenSendFailed/*2*/ = this.defaultMQProducer.getRetryTimesWhenSendFailed();
            int timesTotal = communicationMode == CommunicationMode.SYNC ?
                    1 + retryTimesWhenSendFailed/*2*/ : // 同步发送 总次数 默认 3
                    1; // 非同步发送
            // 当前是第几次发送
            int times = 0;
            // 下标值 代表 发送的 第几次，值 代表这次选择的 brokerName
            // 存储每次发送的时候选择的 brokerName
            String[/*存储每次发送的时候选择的 brokerName*/] brokersSent = new String[timesTotal];

            for (; times < timesTotal; times++/* 上次发送失败，则重试 */) {// 循环发送，什么时候跳出循环？1.发送成功 2.次数超过上限
                // 上次发送时候的 brokerName
                String lastBrokerName/*上次发送的brokerName,第一次发的时候为 null*/ = (null == mq/*第一次发送的时候mq肯定是null*/) ? null : mq.getBrokerName();

                // {"topic":"主题","brokerName":"broker-a","queueId":"1"}
                MessageQueue mqSelected = this.selectOneMessageQueue/*从发布信息中选择队列*/(topicPublishInfo, lastBrokerName);
                if (mqSelected != null) {// 选择到了一个合适的发送队列
                    mq = mqSelected;
                    brokersSent[times/*存储本次发送的 brokerName*/] = mq.getBrokerName();
                    try {
                        beginTimestampPrev/*记录本轮发送的开始时间*/ = System.currentTimeMillis();
                        if (times > 0) {
                            //Reset topic with namespace during resend.
                            String topicWithNamespace = this.defaultMQProducer.withNamespace(msg.getTopic());
                            msg.setTopic(topicWithNamespace);
                        }
                        long costTime = beginTimestampPrev - beginTimestampFirst;
                        if (timeout < costTime) {
                            // 计算当前耗时
                            callTimeout = true;
                            break;
                        }

                        // 发送消息的核心方法！！！！！！！！
                        sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout - costTime);
                        // 本轮发送结束时间
                        endTimestamp = System.currentTimeMillis();
                        // 更新容错策略,如果没开启，则忽略
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        switch (communicationMode) {
                            case ASYNC:
                                // 异步直接返回null，结果交给回调处理
                                return null;
                            case ONEWAY:
                                // 单向直接返回null，单向没有返回值
                                return null;
                            case SYNC:
                                // 同步
                                /**
                                 *     SEND_OK,
                                 *     // 刷盘超时
                                 *     FLUSH_DISK_TIMEOUT,
                                 *     // 更新到 slave 超时
                                 *     FLUSH_SLAVE_TIMEOUT,
                                 *     // 配置了 slave，但是 slave 掉线了
                                 *     SLAVE_NOT_AVAILABLE,
                                 */
                                if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                    // 同步发送没有成功，说明服务端 broker 存储失败
                                    if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()/*一般设置为 true*/) {
                                        // 当存储失败的是需要继续发送，则继续
                                        // 重试
                                        continue;
                                    }
                                }
                                // 正常情况，发送成功则返回结果了，循环也结束了
                                return sendResult;
                            default:
                                break;
                        }
                    } catch (RemotingException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        // 重试
                        continue;
                    } catch (MQClientException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        // 重试
                        continue;
                    } catch (MQBrokerException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        switch (e.getResponseCode()) {
                            case ResponseCode.TOPIC_NOT_EXIST:
                            case ResponseCode.SERVICE_NOT_AVAILABLE:
                            case ResponseCode.SYSTEM_ERROR:
                            case ResponseCode.NO_PERMISSION:
                            case ResponseCode.NO_BUYER_ID:
                            case ResponseCode.NOT_IN_CURRENT_UNIT:
                                // 重试
                                continue;
                            default:
                                if (sendResult != null) {
                                    return sendResult;
                                }
                                // 阻断
                                throw e;
                        }
                    } catch (InterruptedException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        log.warn(String.format("sendKernelImpl exception, throw exception, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        log.warn("sendKernelImpl exception", e);
                        log.warn(msg.toString());
                        // 阻断
                        throw e;
                    }
                } else {
                    break;
                }
            }

            /*
             * 执行到这里
             * 1. for循环达到上限还是没成功
             * 2. sendKernelImpl 方法异常
             * 3. 发送超时
             */

            if (sendResult != null) {
                return sendResult;
            }
            String info = String.format("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: %s", times, System.currentTimeMillis() - beginTimestampFirst, msg.getTopic(), Arrays.toString(brokersSent));
            info += FAQUrl.suggestTodo(FAQUrl.SEND_MSG_FAILED);
            MQClientException mqClientException = new MQClientException(info, exception);
            if (callTimeout) {
                throw new RemotingTooMuchRequestException("sendDefaultImpl call timeout");
            }
            if (exception instanceof MQBrokerException) {
                mqClientException.setResponseCode(((MQBrokerException) exception).getResponseCode());
            } else if (exception instanceof RemotingConnectException) {
                mqClientException.setResponseCode(ClientErrorCode.CONNECT_BROKER_EXCEPTION);
            } else if (exception instanceof RemotingTimeoutException) {
                mqClientException.setResponseCode(ClientErrorCode.ACCESS_BROKER_TIMEOUT);
            } else if (exception instanceof MQClientException) {
                mqClientException.setResponseCode(ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION);
            }
            throw mqClientException;
        }

        validateNameServerSetting();

        // 未找到当前主题的路由数据，没有办法发送消息
        throw new MQClientException("No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO), null).setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION);
    }

    private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        // 从生产者本地发布信息映射表中尝试获取发布信息
        // ConcurrentMap<String/*topic*/, TopicPublishInfo/*该主题的发布信息*/>
        TopicPublishInfo topicPublishInfo/*该主题的发布信息*/ = this.topicPublishInfoTable.get(topic);

        // 如果本地获取不到或者状态不对
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            // 说明生产者本地保存的指定 topic 的发布信息是空的，需要从 客户端获取发布信息

            // 向生产者本地保存一份该 topic 的空的发布信息
            this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());

            // 调用客户端的方法：从namesrv 上更新指定主题的路由数据
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);

            // 正常情况下，这一步拿到的主题发布信息是有数据的
            // 特殊情况，因为 namesrv 上没有该 topic 的路由数据，所以在这里拿到的还是空对象

            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }

        if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {

            // 正常情况
            return topicPublishInfo;
        } else {
            /**
             * 本地没有，namesrv也没有的情况，因为在发送消息之前，没有提前在broker上创建 topic 的路由数据
             */
            // 上面从 namesrv 也没有拿到该主题的路由信息，则会进入该分支

            // 特殊情况
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, this.defaultMQProducer);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
            return topicPublishInfo;
        }
    }

    // 发送 核心 实现
    private SendResult sendKernelImpl(final Message msg, final MessageQueue mq, final CommunicationMode communicationMode, final SendCallback sendCallback, final TopicPublishInfo topicPublishInfo, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        long beginStartTime = System.currentTimeMillis();
        String brokerName = mq.getBrokerName();
        // 获取指定 brokerName 主机的地址,master节点
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(brokerName);
        if (null == brokerAddr) {
            tryToFindTopicPublishInfo(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(brokerName);
        }
        // 执行 endMsgHook 的时候使用
        SendMessageContext context = null;
        if (brokerAddr != null) {
            // 当前生产者是否走 vip 通道？如果走，则地址换换就好了，具体的发送逻辑还是一样的
            boolean sendMessageWithVIPChannel = this.defaultMQProducer.isSendMessageWithVIPChannel();
            brokerAddr = MixAll.brokerVIPChannel(sendMessageWithVIPChannel, brokerAddr);

            //  TODO ????干嘛的呢？ 消息
            byte[] prevBody/*消息体*/ = msg.getBody();
            try {
                //for MessageBatch,ID has been set in the generating process
                if (!(msg instanceof MessageBatch)) {

                    /**
                     * 给消息生成一个唯一Id，在 msg.properties("UNIQ_KEY"m "msgId")
                     * 服务器 broker 会给消息安装 UNIQ_KEY 建立一个 hash 索引(IndexFile文件)
                     *
                     * broker会根据msgId建立一个HASH索引
                     */
                    MessageClientIDSetter.setUniqID(msg);// 非批量消息则生成唯一id
                }

                boolean topicWithNamespace = false;
                ClientConfig clientConfig = this.mQClientFactory.getClientConfig();
                if (null != clientConfig.getNamespace()) {
                    msg.setInstanceId(clientConfig.getNamespace());
                    topicWithNamespace = true;
                }

                int sysFlag = 0;

                // 是否压缩？
                boolean msgBodyCompressed = false;
                if (this.tryToCompressMessage(msg)/*返回是压缩，并且压缩*/) {
                    // 已经压缩了，然后打一个标记
                    sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
                    msgBodyCompressed = true;
                    // 此时 msg 中的 body 以及是压缩过的
                }

                // 事务消息特有的属性
                final String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED/*TRAN_MSG*/);
                if (tranMsg != null && Boolean.parseBoolean(tranMsg)) {
                    // 半消息
                    sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
                }

                // 用户扩展点，用户可以注册 CheckForbin 控制消息发送
                if (hasCheckForbiddenHook()) {
                    CheckForbiddenContext checkForbiddenContext = new CheckForbiddenContext();
                    checkForbiddenContext.setNameSrvAddr(this.defaultMQProducer.getNamesrvAddr());
                    checkForbiddenContext.setGroup(this.defaultMQProducer.getProducerGroup());
                    checkForbiddenContext.setCommunicationMode(communicationMode);
                    checkForbiddenContext.setBrokerAddr(brokerAddr);
                    checkForbiddenContext.setMessage(msg);
                    checkForbiddenContext.setMq(mq);
                    checkForbiddenContext.setUnitMode(this.isUnitMode());
                    // 可以抛出异常的钩子
                    this.executeCheckForbiddenHook(checkForbiddenContext);
                }

                if (this.hasSendMessageHook()) {
                    // 创建一个 上下文
                    context = new SendMessageContext();
                    context.setProducer(this);
                    context.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                    context.setCommunicationMode(communicationMode);
                    context.setBornHost(this.defaultMQProducer.getClientIP());
                    context.setBrokerAddr(brokerAddr);
                    context.setMessage(msg);
                    context.setMq(mq);
                    context.setNamespace(this.defaultMQProducer.getNamespace());
                    String isTrans = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED/*TRAN_MSG*/);
                    if (isTrans != null && isTrans.equals("true")) {
                        context.setMsgType(MessageType.Trans_Msg_Half);
                    }

                    if (msg.getProperty("__STARTDELIVERTIME") != null || msg.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL/*DELAY*/) != null) {
                        // 延迟
                        context.setMsgType(MessageType.Delay_Msg);
                    }
                    this.executeSendMessageHookBefore(context);
                }

                SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                // 生产者组
                requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                // 主题
                requestHeader.setTopic(msg.getTopic());
                // 缺省主题
                requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey() /* TBW102 */);
                // 默认的主题队列数：4
                requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums() /*4*/);
                // 当前选择的队列id
                requestHeader.setQueueId(mq.getQueueId());
                // 系统标记变量
                requestHeader.setSysFlag(sysFlag);
                // 消息的创建时间
                requestHeader.setBornTimestamp(System.currentTimeMillis());
                // 消息的 flag
                requestHeader.setFlag(msg.getFlag());
                // 消息的 properties
                String properties2String = MessageDecoder.messageProperties2String(msg.getProperties());
                requestHeader.setProperties(properties2String);
                // 消费次数
                requestHeader.setReconsumeTimes(0);
                // 模型
                requestHeader.setUnitMode(this.isUnitMode());
                // 是否批量
                requestHeader.setBatch(msg instanceof MessageBatch);

                // 消息重试的逻辑
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX/*%RETRY%*/)) {
                    // return msg.getProperty(MessageConst.PROPERTY_RECONSUME_TIME/*RECONSUME_TIME*/);
                    String reconsumeTimes/*RECONSUME_TIME*/ = MessageAccessor.getReconsumeTime(msg);
                    if (reconsumeTimes != null) {
                        // 重试次数
                        Integer reTime = Integer.valueOf(reconsumeTimes);
                        requestHeader.setReconsumeTimes(reTime);
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME/*RECONSUME_TIME*/);
                    }

                    // return msg.getProperty(MessageConst.PROPERTY_MAX_RECONSUME_TIMES/*MAX_RECONSUME_TIMES*/);
                    String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
                    if (maxReconsumeTimes != null) {
                        requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
                    }
                }

                // 👆都是准备发送阶段，👇开始发送消息

                // 发送结果
                SendResult sendResult = null;
                MQClientAPIImpl mqClientAPIImpl = this.mQClientFactory.getMQClientAPIImpl();
                switch (communicationMode) {
                    case ASYNC:
                        Message tmpMessage = msg;
                        boolean messageCloned = false;
                        if (msgBodyCompressed) {
                            //If msg body was compressed, msgbody should be reset using prevBody.
                            //Clone new message using commpressed message body and recover origin massage.
                            //Fix bug:https://github.com/apache/rocketmq-externals/issues/66
                            tmpMessage = MessageAccessor.cloneMessage(msg);
                            messageCloned = true;
                            msg.setBody(prevBody/*压缩之前的消息体*/);
                        }

                        if (topicWithNamespace) {
                            if (!messageCloned) {
                                tmpMessage = MessageAccessor.cloneMessage(msg);
                                messageCloned = true;
                            }
                            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
                        }

                        long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeAsync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }

                        int retryTimesWhenSendAsyncFailed = this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed();
                        sendResult = mqClientAPIImpl.sendMessage(
                                brokerAddr, brokerName, tmpMessage, requestHeader, timeout - costTimeAsync,
                                communicationMode, sendCallback/*异步回调*/, topicPublishInfo, this.mQClientFactory, retryTimesWhenSendAsyncFailed, context, this);
                        break;
                    case ONEWAY:
                    case SYNC:
                        // 单向，同步都走这里
                        long costTimeSync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeSync) {
                            // 超时了，干！
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        // ！！！！  发送了，拿到服务端broker返回的结果
                        sendResult = mqClientAPIImpl.sendMessage(brokerAddr, brokerName, msg, requestHeader, timeout - costTimeSync, communicationMode, context, this);
                        break;
                    default:
                        assert false;
                        break;
                }

                if (this.hasSendMessageHook()) {
                    context.setSendResult(sendResult);
                    this.executeSendMessageHookAfter(context);
                }

                return sendResult;
            } catch (RemotingException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } catch (MQBrokerException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } catch (InterruptedException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } finally {
                // 最终恢复压缩之前的消息体
                msg.setBody(prevBody);
                // 最终恢复之前的主题
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    /**
     * ocketmq-cpp/src/producer/DefaultMQProducer.cpp: tryToCompressMessage函数没有判断msg是否被压缩过，只判断了是否过大，如果压缩后发送失败进行重发并且body大小仍然>=getCompressMsgBodyOverHowmuch()， body会被压缩两次
     *
     * @param msg
     * @return
     * @see {@link https://github.com/apache/rocketmq-externals/issues/66}
     */
    private boolean tryToCompressMessage(final Message msg) {
        if (msg instanceof MessageBatch) {
            //batch dose not support compressing right now
            return false;
        }
        byte[] body = msg.getBody();
        if (body != null) {
            int compressMsgBodyOverHowmuch = this.defaultMQProducer.getCompressMsgBodyOverHowmuch();
            if (body.length >= compressMsgBodyOverHowmuch) {
                try {

                    // 压缩
                    byte[] data = UtilAll.compress(body, zipCompressLevel);
                    if (data != null) {
                        msg.setBody(data);
                        return true;
                    }
                } catch (IOException e) {
                    log.error("tryToCompressMessage exception", e);
                    log.warn(msg.toString());
                }
            }
        }

        return false;
    }

    public boolean hasCheckForbiddenHook() {
        return !checkForbiddenHookList.isEmpty();
    }

    public void executeCheckForbiddenHook(final CheckForbiddenContext context) throws MQClientException {
        if (hasCheckForbiddenHook()) {
            for (CheckForbiddenHook hook : checkForbiddenHookList) {
                hook.checkForbidden(context);
            }
        }
    }

    public boolean hasSendMessageHook() {
        return !this.sendMessageHookList.isEmpty();
    }

    public void executeSendMessageHookBefore(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageBefore(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookBefore", e);
                }
            }
        }
    }

    public void executeSendMessageHookAfter(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookAfter", e);
                }
            }
        }
    }

    /**
     * DEFAULT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendDefaultImpl(msg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * KERNEL SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueue mq) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, mq, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueue mq, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        if (!msg.getTopic().equals(mq.getTopic())) {
            // 消息的 topic 跟 队列的 topic 不一样，报错

            throw new MQClientException("message's topic not equal mq's topic", null);
        }

        long costTime = System.currentTimeMillis() - beginStartTime;
        if (timeout < costTime) {
            throw new RemotingTooMuchRequestException("call timeout");
        }

        return this.sendKernelImpl(msg, mq, CommunicationMode.SYNC, null, null, timeout);
    }

    /**
     * KERNEL ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        send(msg, mq, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param mq
     * @param sendCallback
     * @param timeout the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @Deprecated
    public void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        makeSureStateOK();
                        Validators.checkMessage(msg, defaultMQProducer);

                        if (!msg.getTopic().equals(mq.getTopic())) {
                            throw new MQClientException("message's topic not equal mq's topic", null);
                        }
                        long costTime = System.currentTimeMillis() - beginStartTime;
                        if (timeout > costTime) {
                            try {
                                sendKernelImpl(msg, mq, CommunicationMode.ASYNC, sendCallback, null, timeout - costTime);
                            } catch (MQBrokerException e) {
                                throw new MQClientException("unknown exception", e);
                            }
                        } else {
                            sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                        }
                    } catch (Exception e) {
                        sendCallback.onException(e);
                    }

                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }

    }

    /**
     * KERNEL ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg, MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        try {
            this.sendKernelImpl(msg, mq, CommunicationMode.ONEWAY, null, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * SELECT SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, selector, arg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null, timeout);
    }

    private SendResult sendSelectImpl(Message msg, MessageQueueSelector selector, Object arg, final CommunicationMode communicationMode, final SendCallback sendCallback, final long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            MessageQueue mq = null;
            try {
                MQAdminImpl mqAdminImpl = mQClientFactory.getMQAdminImpl();
                List<MessageQueue> queueList = topicPublishInfo.getMessageQueueList();

                // 类似复制了一个集合，数据都是一样的
                List<MessageQueue> messageQueueList = mqAdminImpl.parsePublishMessageQueues(queueList);
                Message userMessage = MessageAccessor.cloneMessage(msg);
                String topic = userMessage.getTopic();
                ClientConfig clientConfig = mQClientFactory.getClientConfig();
                String namespace = clientConfig.getNamespace();
                String userTopic = NamespaceUtil.withoutNamespace(topic, namespace);
                userMessage.setTopic(userTopic);

                // 根据队列选择器选择一个待发送队列
                MessageQueue messageQueue = selector.select(messageQueueList, userMessage, arg);
                mq = clientConfig.queueWithNamespace(messageQueue);
            } catch (Throwable e) {
                throw new MQClientException("select message queue throwed exception.", e);
            }

            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeout < costTime) {
                throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
            }
            if (mq != null) {
                return this.sendKernelImpl(msg, mq, communicationMode, sendCallback, null, timeout - costTime);
            } else {
                throw new MQClientException("select message queue return null.", null);
            }
        }

        validateNameServerSetting();
        throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
    }

    /**
     * SELECT ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        send(msg, selector, arg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param selector
     * @param arg
     * @param sendCallback
     * @param timeout the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @Deprecated
    public void send(final Message msg, final MessageQueueSelector selector, final Object arg, final SendCallback sendCallback, final long timeout) throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            try {
                                sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, sendCallback, timeout - costTime);
                            } catch (MQBrokerException e) {
                                throw new MQClientException("unknownn exception", e);
                            }
                        } catch (Exception e) {
                            sendCallback.onException(e);
                        }
                    } else {
                        sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                    }
                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("exector rejected ", e);
        }
    }

    /**
     * SELECT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * 发送事务消息
     */
    public TransactionSendResult sendMessageInTransaction(final Message msg, final LocalTransactionExecuter localTransactionExecuter, final Object arg) throws MQClientException {
        TransactionListener transactionListener = getCheckListener();
        if (null == localTransactionExecuter && null == transactionListener) {
            // 肯定不能全部为空呀！！！！！
            throw new MQClientException("tranExecutor is null", null);
        }

        // ignore DelayTimeLevel parameter
        // 事务消息不支持延迟消息！！！！
        if (msg.getDelayTimeLevel() != 0) {
            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        }

        Validators.checkMessage(msg, this.defaultMQProducer);

        SendResult sendResult = null;
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED /*TRAN_MSG*/, "true");
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP /*PGROUP*/, this.defaultMQProducer.getProducerGroup());
        try {
            sendResult = this.send(msg);
        } catch (Exception e) {
            throw new MQClientException("send message Exception", e);
        }

        LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
        Throwable localException = null;
        SendStatus sendStatus = sendResult.getSendStatus();
        switch (sendStatus) {
            case SEND_OK: {
                // 如果消息发送成功； 则需要执行本地事务！！！！

                try {
                    if (sendResult.getTransactionId() != null) {
                        msg.putUserProperty("__transactionId__", sendResult.getTransactionId());
                    }
                    String transactionId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX /*UNIQ_KEY*/);
                    if (null != transactionId && !"".equals(transactionId)) {
                        msg.setTransactionId(transactionId);
                    }
                    if (null != localTransactionExecuter) {
                        localTransactionState = localTransactionExecuter.executeLocalTransactionBranch(msg, arg);
                    } else if (transactionListener != null) {
                        log.debug("Used new transaction API");
                        localTransactionState = transactionListener.executeLocalTransaction(msg, arg);
                    }
                    if (null == localTransactionState) {
                        localTransactionState = LocalTransactionState.UNKNOW;
                    }

                    if (localTransactionState != LocalTransactionState.COMMIT_MESSAGE) {
                        log.info("executeLocalTransactionBranch return {}", localTransactionState);
                        log.info(msg.toString());
                    }
                } catch (Throwable e) {
                    log.info("executeLocalTransactionBranch exception", e);
                    log.info(msg.toString());
                    localException = e;
                }
            }
            break;
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case SLAVE_NOT_AVAILABLE:
                localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
                break;
            default:
                break;
        }

        try {

            // 本地事务结果拿到之后，就进行二阶段提交！！！！

            this.endTransaction(sendResult/*half消息发送结果*/, localTransactionState/*本地事务执行结果*/, localException/*本地事务执行的时候出现的异常*/);
        } catch (Exception e) {
            log.warn("local transaction execute " + localTransactionState + ", but end broker transaction failed", e);
        }

        TransactionSendResult transactionSendResult = new TransactionSendResult();
        transactionSendResult.setSendStatus(sendStatus);
        transactionSendResult.setMessageQueue(sendResult.getMessageQueue());
        transactionSendResult.setMsgId(sendResult.getMsgId());
        transactionSendResult.setQueueOffset(sendResult.getQueueOffset());
        transactionSendResult.setTransactionId(sendResult.getTransactionId());
        transactionSendResult.setLocalTransactionState(localTransactionState);
        return transactionSendResult;
    }

    /**
     * DEFAULT SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        // 默认3s
        int sendMsgTimeout = this.defaultMQProducer.getSendMsgTimeout();
        return send(msg, sendMsgTimeout);
    }

    private void endTransaction(final SendResult sendResult/*half消息发送结果*/, final LocalTransactionState localTransactionState/*本地事务执行结果*/, final Throwable localException/*本地事务执行的时候出现的异常*/)
            throws RemotingException, MQBrokerException, InterruptedException, UnknownHostException {

        final MessageId id;
        String offsetMsgId = sendResult.getOffsetMsgId();
        if (offsetMsgId != null) {
            id = MessageDecoder.decodeMessageId(offsetMsgId);
        } else {
            id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
        }
        String transactionId = sendResult.getTransactionId();
        final String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(sendResult.getMessageQueue().getBrokerName());
        EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
        requestHeader.setTransactionId(transactionId);
        requestHeader.setCommitLogOffset(id.getOffset());
        switch (localTransactionState) {
            case COMMIT_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                break;
            case ROLLBACK_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                break;
            case UNKNOW:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                break;
            default:
                break;
        }

        requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
        requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
        requestHeader.setMsgId(sendResult.getMsgId());
        String remark = localException != null ? ("executeLocalTransactionBranch exception: " + localException.toString()) : null;
        MQClientAPIImpl mqClientAPIImpl = this.mQClientFactory.getMQClientAPIImpl();
        int sendMsgTimeout = this.defaultMQProducer.getSendMsgTimeout();
        mqClientAPIImpl.endTransactionOneway(brokerAddr, requestHeader, remark, sendMsgTimeout);
    }

    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.mQClientFactory.getMQClientAPIImpl().getRemotingClient().setCallbackExecutor(callbackExecutor);
    }

    public ExecutorService getAsyncSenderExecutor() {
        return null == asyncSenderExecutor ? defaultAsyncSenderExecutor : asyncSenderExecutor;
    }

    public void setAsyncSenderExecutor(ExecutorService asyncSenderExecutor) {
        this.asyncSenderExecutor = asyncSenderExecutor;
    }

    public SendResult send(Message msg, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null/*同步发送不需要*/, timeout);
    }

    // request 发送的消息需要消费者回执一条消息
    public Message request(Message msg, long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);

        // uuid
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID/*CORRELATION_ID*/);

        ConcurrentHashMap<String, RequestResponseFuture> requestFutureTable = RequestFutureTable.getRequestFutureTable();
        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            // 塞入
            requestFutureTable.put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            // 发送线程在此阻塞！！！
            // 等待消费者消费完成
            Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);

            // 返回结果或者超时
            if (responseMessage == null) {
                if (requestResponseFuture.isSendRequestOk()) {
                    throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
                } else {
                    throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
                }
            }
            return responseMessage;
        } finally {
            requestFutureTable.remove(correlationId);
        }
    }

    public void request(Message msg, final RequestCallback requestCallback, long timeout) throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);
    }

    public Message request(final Message msg, final MessageQueueSelector selector, final Object arg, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
            if (responseMessage == null) {
                if (requestResponseFuture.isSendRequestOk()) {
                    throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
                } else {
                    throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
                }
            }
            return responseMessage;
        } finally {
            RequestFutureTable.getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(final Message msg, final MessageQueueSelector selector, final Object arg, final RequestCallback requestCallback, final long timeout)
            throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);

    }

    public Message request(final Message msg, final MessageQueue mq, final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID/*CORRELATION_ID*/);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, null, timeout - cost);

            Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
            if (responseMessage == null) {
                if (requestResponseFuture.isSendRequestOk()) {
                    throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
                } else {
                    throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
                }
            }
            return responseMessage;
        } finally {
            RequestFutureTable.getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout) throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, null, timeout - cost);
    }

    private void requestFail(final String correlationId) {
        RequestResponseFuture responseFuture = RequestFutureTable.getRequestFutureTable().remove(correlationId);
        if (responseFuture != null) {
            responseFuture.setSendRequestOk(false);
            responseFuture.putResponseMessage(null);
            try {
                responseFuture.executeRequestCallback();
            } catch (Exception e) {
                log.warn("execute requestCallback in requestFail, and callback throw", e);
            }
        }
    }

    private void prepareSendRequest(final Message msg, long timeout) {

        // UUID.randomUUID().toString();
        String correlationId = CorrelationIdUtil.createCorrelationId();

        MQClientInstance mqClientInstance = this.getmQClientFactory();

        // 客户端Id：ip&pid
        String requestClientId = mqClientInstance.getClientId();
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID/*CORRELATION_ID*/, correlationId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT/*REPLY_TO_CLIENT*/, requestClientId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL/*TTL*/, String.valueOf(timeout));

        // ConcurrentMap<String/* Topic */, TopicRouteData/*主题路由信息*/>
        ConcurrentMap<String, TopicRouteData> topicRouteTable = mqClientInstance.getTopicRouteTable();
        boolean hasRouteData = topicRouteTable.containsKey(msg.getTopic());
        if (!hasRouteData) {
            long beginTimestamp = System.currentTimeMillis();
            this.tryToFindTopicPublishInfo(msg.getTopic());
            mqClientInstance.sendHeartbeatToAllBrokerWithLock();
            long cost = System.currentTimeMillis() - beginTimestamp;
            if (cost > 500) {
                log.warn("prepare send request for <{}> cost {} ms", msg.getTopic(), cost);
            }
        }
    }

    public long[] getNotAvailableDuration() {
        return this.mqFaultStrategy.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.mqFaultStrategy.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.mqFaultStrategy.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.mqFaultStrategy.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.mqFaultStrategy.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.mqFaultStrategy.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }
}

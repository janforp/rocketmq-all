package org.apache.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filter.ExpressionForRetryMessageFilter;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.longpolling.PullRequest;
import org.apache.rocketmq.broker.longpolling.PullRequestHoldService;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.pagecache.ManyMessageTransfer;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.topic.OffsetMovedEvent;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;

@SuppressWarnings("all")
public class PullMessageProcessor /*extends AsyncNettyRequestProcessor */ implements NettyRequestProcessor {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    private List<ConsumeMessageHook> consumeMessageHookList;

    public PullMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true/*允许本次拉消息长轮询*/);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand processRequest(final Channel channel, RemotingCommand request/*有 PullMessageRequestHeader 对象*/, boolean brokerAllowSuspend /*是否允许本次拉消息长轮询*/) throws RemotingCommandException {
        // 创建响应对象
        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
        // 拿到刚才上面创建的空对象
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
        // 解码请求对象
        final PullMessageRequestHeader requestHeader = (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);
        // 设置请求id
        response.setOpaque(request.getOpaque()/*客户端需要根据该值找到对应的 responseFuture 完成后面的事情*/);

        BrokerConfig brokerConfig = this.brokerController.getBrokerConfig();
        if (!PermName.isReadable(brokerConfig.getBrokerPermission())/*当前broker的权限*/) {
            // 不可读
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] pulling message is forbidden", brokerConfig.getBrokerIP1()));
            return response;
        }

        SubscriptionGroupManager subscriptionGroupManager = this.brokerController.getSubscriptionGroupManager();
        // 订阅组配置
        String consumerGroup = requestHeader.getConsumerGroup();
        SubscriptionGroupConfig subscriptionGroupConfig = subscriptionGroupManager.findSubscriptionGroupConfig(consumerGroup);
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s", consumerGroup, FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        if (!subscriptionGroupConfig.isConsumeEnable()) { // 当前消费组不可消费
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + consumerGroup);
            return response;
        }

        // 解析 SysFlag，是否允许服务器端长轮询
        final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
        // 客户端是否提交了 offset
        final boolean hasCommitOffsetFlag = PullSysFlag.hasCommitOffsetFlag(requestHeader.getSysFlag());
        // 是否有过滤表达式
        final boolean hasSubscriptionFlag = PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag());
        // 长轮询超时时间，一般15s
        final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

        // 主题配置
        TopicConfigManager topicConfigManager = this.brokerController.getTopicConfigManager();
        String topicInRequest = requestHeader.getTopic();
        TopicConfig topicConfig = topicConfigManager.selectTopicConfig(topicInRequest);
        if (null == topicConfig) {
            log.error("the topic {} not exist, consumer: {}", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            // 不可读
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] pulling message is forbidden");
            return response;
        }

        Integer queueIdInHeader = requestHeader.getQueueId();
        if (queueIdInHeader < 0 /* queueId  >= 0 */ || queueIdInHeader >= topicConfig.getReadQueueNums() /* queueId 是从 0 开始计数的，最大不过 readQueueNums - 1 个 */) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]", queueIdInHeader, requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }

        SubscriptionData subscriptionData;
        ConsumerFilterData consumerFilterData = null;
        if (hasSubscriptionFlag) {
            try {
                subscriptionData = FilterAPI.build(requestHeader.getTopic(), requestHeader.getSubscription(), requestHeader.getExpressionType());
                if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    consumerFilterData = ConsumerFilterManager.build(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getSubscription(), requestHeader.getExpressionType(), requestHeader.getSubVersion());
                    assert consumerFilterData != null;
                }
            } catch (Exception e) {
                log.warn("Parse the consumer's subscription[{}] failed, group: {}", requestHeader.getSubscription(), requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }
        } else {
            // 没有过滤表达式

            // 消费者组信息，主要包含每个主题的订阅详情
            ConsumerManager consumerManager = this.brokerController.getConsumerManager();
            ConsumerGroupInfo consumerGroupInfo = consumerManager.getConsumerGroupInfo(topicInRequest);
            if (null == consumerGroupInfo) {
                log.warn("the consumer's group info not exist, group: {}", requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's group info not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            if (!subscriptionGroupConfig.isConsumeBroadcastEnable() && consumerGroupInfo.getMessageModel() == MessageModel.BROADCASTING) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("the consumer group[" + requestHeader.getConsumerGroup() + "] can not consume by broadcast way");
                return response;
            }

            // 从消费者组中提取出来当前请求主题的订阅数据
            subscriptionData = consumerGroupInfo.findSubscriptionData(topicInRequest);
            if (null == subscriptionData) {
                log.warn("the consumer's subscription not exist, group: {}, topic:{}", requestHeader.getConsumerGroup(), requestHeader.getTopic());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's subscription not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            if (subscriptionData.getSubVersion() < requestHeader.getSubVersion()) {
                log.warn("The broker's subscription is not latest, group: {} {}", requestHeader.getConsumerGroup(), subscriptionData.getSubString());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_LATEST);
                response.setRemark("the consumer's subscription not latest");
                return response;
            }
            if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                // 不是 TAG 过滤模式

                consumerFilterData = this.brokerController.getConsumerFilterManager().get(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                if (consumerFilterData == null) {
                    response.setCode(ResponseCode.FILTER_DATA_NOT_EXIST);
                    response.setRemark("The broker's consumer filter data is not exist!Your expression may be wrong!");
                    return response;
                }
                if (consumerFilterData.getClientVersion() < requestHeader.getSubVersion()) {
                    log.warn("The broker's consumer filter data is not latest, group: {}, topic: {}, serverV: {}, clientV: {}",
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(), consumerFilterData.getClientVersion(), requestHeader.getSubVersion());
                    response.setCode(ResponseCode.FILTER_DATA_NOT_LATEST);
                    response.setRemark("the consumer's consumer filter data not latest");
                    return response;
                }
            }
        }

        if (!ExpressionType.isTagType(subscriptionData.getExpressionType()) && !brokerConfig.isEnablePropertyFilter()) {
            // 不是 TAG 并且服务器不支持其他过滤模式，则不行
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
            return response;
        }

        MessageFilter messageFilter;
        if (brokerConfig.isFilterSupportRetry()) {
            messageFilter = new ExpressionForRetryMessageFilter(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager());
        } else {
            messageFilter = new ExpressionMessageFilter/*TAG CODE 过滤*/(subscriptionData, consumerFilterData, this.brokerController.getConsumerFilterManager());
        }

        MessageStore messageStore = this.brokerController.getMessageStore();

        // 通过 messageStore 去查询消息 TODO @@@!!!!
        final GetMessageResult getMessageResult = messageStore.getMessage(requestHeader.getConsumerGroup(), requestHeader.getTopic(), queueIdInHeader, requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(), messageFilter);

        if (getMessageResult != null) {
            // 查询结果不为空

            GetMessageStatus status = getMessageResult.getStatus();
            response.setRemark(status.name());
            // 返回给客户端，表示下一次再向当前队列拉消息的时候的开始 offset
            responseHeader.setNextBeginOffset(getMessageResult.getNextBeginOffset()/*表示下一次再向当前队列拉消息的时候的开始 offset*/);
            responseHeader.setMinOffset(getMessageResult.getMinOffset()/*当前查询队列的最小*/);
            responseHeader.setMaxOffset(getMessageResult.getMaxOffset()/*当前查询队列的最大*/);

            // 设置推荐下次拉消息的节点
            if (getMessageResult.isSuggestPullingFromSlave()) {
                // 建议下次从从 broker 拿消息，则设置为1
                responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly()/* 默认 就是 1*/);
            } else {
                // 还是从主节点拿消息，设置为0
                responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
            }

            MessageStoreConfig messageStoreConfig = this.brokerController.getMessageStoreConfig();
            switch (messageStoreConfig.getBrokerRole()) { // 当前服务器节点的角色
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    break;
                case SLAVE:

                    // 如果当前服务器主机节点是 slave 并且 从节点读 未开启的话，直接给客户端一个主题 "PULL_RETRY_IMMEDIATELY"
                    // 客户端 检查是该状态后，会立马重新再次发起 pull 请求，此时使用的 brokerId 为 MASTER_ID
                    if (!brokerConfig.isSlaveReadEnable()) {

                        // 这代码写得是 有问题，上面都已经查询到结果了，这里做这个判断干嘛？？
                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                        responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
                    }
                    break;
            }

            if (brokerConfig.isSlaveReadEnable()/*支持从 slave 节点读开启了 */) {
                // 从节点读开启拉

                // consume too slow ,redirect to another machine
                if (getMessageResult.isSuggestPullingFromSlave()) {
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
                }
                // consume ok
                else {
                    responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                }
            } else {
                responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
            }

            // 状态码转换
            switch (status /* GetMessageStatus */) {
                case FOUND:
                    response.setCode(ResponseCode.SUCCESS);
                    break;
                case MESSAGE_WAS_REMOVING:
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    break;
                case NO_MATCHED_LOGIC_QUEUE:
                case NO_MESSAGE_IN_QUEUE:
                    if (0 != requestHeader.getQueueOffset()) {
                        response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                        // XXX: warn and notify me
                        String msg = "the broker store no queue data, fix the request offset {} to {}, Topic: {} QueueId: {} Consumer Group: {}";
                        log.info(msg, requestHeader.getQueueOffset(), getMessageResult.getNextBeginOffset(), requestHeader.getTopic(), queueIdInHeader, requestHeader.getConsumerGroup());
                    } else {
                        response.setCode(ResponseCode.PULL_NOT_FOUND);
                    }
                    break;
                case NO_MATCHED_MESSAGE:
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    break;
                case OFFSET_FOUND_NULL:
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                case OFFSET_OVERFLOW_BADLY:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    // XXX: warn and notify me
                    log.info("the request offset: {} over flow badly, broker max offset: {}, consumer: {}", requestHeader.getQueueOffset(), getMessageResult.getMaxOffset(), channel.remoteAddress());
                    break;
                case OFFSET_OVERFLOW_ONE:
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                case OFFSET_TOO_SMALL:
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    String msg = "the request offset too small. group={}, topic={}, requestOffset={}, brokerMinOffset={}, clientIp={}";
                    log.info(msg, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueOffset(), getMessageResult.getMinOffset(), channel.remoteAddress());
                    break;
                default:
                    assert false;
                    break;
            }

            // 经检查，服务器端并没有注册消费 hook,这里应该是开发人员留给自己的扩展点
            if (this.hasConsumeMessageHook()) {
                ConsumeMessageContext context = new ConsumeMessageContext();
                context.setConsumerGroup(requestHeader.getConsumerGroup());
                context.setTopic(requestHeader.getTopic());
                context.setQueueId(queueIdInHeader);

                String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER/*Owner*/);

                switch (response.getCode()) {
                    case ResponseCode.SUCCESS:
                        int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                        int incValue = getMessageResult.getMsgCount4Commercial() * commercialBaseCount;

                        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_SUCCESS);
                        context.setCommercialRcvTimes(incValue);
                        context.setCommercialRcvSize(getMessageResult.getBufferTotalSize());
                        context.setCommercialOwner(owner);

                        break;
                    case ResponseCode.PULL_NOT_FOUND:
                        if (!brokerAllowSuspend) {

                            context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
                            context.setCommercialRcvTimes(1);
                            context.setCommercialOwner(owner);

                        }
                        break;
                    case ResponseCode.PULL_RETRY_IMMEDIATELY:
                    case ResponseCode.PULL_OFFSET_MOVED:
                        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
                        context.setCommercialRcvTimes(1);
                        context.setCommercialOwner(owner);
                        break;
                    default:
                        assert false;
                        break;
                }

                this.executeConsumeMessageHookBefore(context);
            }

            switch (response.getCode() /* RemotingCommand response 该对象中的 code 字段是根据上面的查询消息结果 GetMessageStatus status 转换来的 */) {
                case ResponseCode.SUCCESS: // 查询成功
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(), getMessageResult.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetSize(requestHeader.getConsumerGroup(), requestHeader.getTopic(), getMessageResult.getBufferTotalSize());
                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(getMessageResult.getMessageCount());

                    // 从堆内转换
                    if (brokerConfig.isTransferMsgByHeap()) {
                        final long beginTimeMills = this.brokerController.getMessageStore().now();

                        // 拿到本次 pull 出来的全部消息的字节
                        final byte[] r/*将本次拉取的全部消息转到字节数组*/ = this.readGetMessageResult(getMessageResult, requestHeader.getConsumerGroup(), requestHeader.getTopic(), queueIdInHeader);
                        BrokerStatsManager brokerStatsManager = this.brokerController.getBrokerStatsManager();
                        brokerStatsManager.incGroupGetLatency(requestHeader.getConsumerGroup(), requestHeader.getTopic(), queueIdInHeader, (int) (this.brokerController.getMessageStore().now() - beginTimeMills));

                        // 将消息数组保存到响应
                        response.setBody(r);
                    } else {
                        try {

                            ByteBuffer byteBuffer = response.encodeHeader(getMessageResult.getBufferTotalSize());
                            FileRegion fileRegion = new ManyMessageTransfer(byteBuffer, getMessageResult);
                            channel.writeAndFlush(fileRegion).addListener((ChannelFutureListener) future -> {
                                getMessageResult.release();
                                if (!future.isSuccess()) {
                                    log.error("transfer many message by pagecache failed, {}", channel.remoteAddress(), future.cause());
                                }
                            });
                        } catch (Throwable e) {
                            log.error("transfer many message by pagecache exception", e);
                            getMessageResult.release();
                        }

                        // TODO 将 response 设置为 null ?
                        response = null;
                    }
                    break;
                case ResponseCode.PULL_NOT_FOUND:

                    /**
                     * OFFSET_OVERFLOW_ONE
                     * 大部分情况下都是因为：pullRequest.offset == queue.maxOffset
                     * queue.maxOffset 计算方式： dataSize / unit_size(20)
                     * 举个例子：当 dataSize 内存储一条 CQData 的时候， dataSize = 20
                     * 则 offset = dataSize / 20 = 1
                     * 当客户端 offset == 1 时，来到服务器拉消息，getMessage(...)方法 首先查询 CQ 数据
                     * offset(1) * 20 = 20 这个物理位点，到该位点查询到的数据，肯定得到 null，因为 CQ 有效数据范围是 0 -20
                     * 所以，这种情况下就是 PULL_NOT_FOUND
                     * 这里需要进行长轮询，否则如果直接返回给客户端，客户端那边会再次立马发起 pull 请求,导致频繁的网络调用！！！
                     */
                    if (brokerAllowSuspend /*服务端是否允许长轮询*/ && hasSuspendFlag/*客户端也设置要求服务端长轮询*/) {

                        // 长轮询的时间，一般是 15 秒
                        long pollingTimeMills = suspendTimeoutMillisLong;
                        if (!brokerConfig.isLongPollingEnable() /*如果服务器没有开启长轮询，则只能端轮询*/) {
                            // 改成短轮询，1秒
                            pollingTimeMills = brokerConfig.getShortPollingTimeMills();
                        }

                        String topic = requestHeader.getTopic();
                        long offset = requestHeader.getQueueOffset();
                        int queueId = queueIdInHeader;
                        // 创建 长轮询请求对象
                        PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills, this.brokerController.getMessageStore().now(), offset, subscriptionData, messageFilter);

                        // 拿到服务
                        PullRequestHoldService pullRequestHoldService = this.brokerController.getPullRequestHoldService();
                        // 在服务端发起轮询，避免直接返回给客户端，导致频繁的网络调用
                        pullRequestHoldService.suspendPullRequest(topic, queueId, pullRequest);

                        /**
                         * TODO 为什么将 response 设置为 null ?
                         * 设置为空之后，外层 requestTask 拿到的结果就是 null,这样 requestTask 内部的 callBack 就不会给客户端发送任何数据了，请求就会挂起(hold)
                         */
                        response = null;
                        break;
                    }

                case ResponseCode.PULL_RETRY_IMMEDIATELY:
                    break;
                case ResponseCode.PULL_OFFSET_MOVED:
                    // 发送一个事件到系统主题中，跟主要逻辑没关系
                    if (messageStoreConfig.getBrokerRole() != BrokerRole.SLAVE || messageStoreConfig.isOffsetCheckInSlave()) {
                        MessageQueue mq = new MessageQueue();
                        mq.setTopic(requestHeader.getTopic());
                        mq.setQueueId(queueIdInHeader);
                        mq.setBrokerName(brokerConfig.getBrokerName());

                        OffsetMovedEvent event = new OffsetMovedEvent();
                        event.setConsumerGroup(requestHeader.getConsumerGroup());
                        event.setMessageQueue(mq);
                        event.setOffsetRequest(requestHeader.getQueueOffset());
                        event.setOffsetNew(getMessageResult.getNextBeginOffset());

                        // 发送事件 到系统主题
                        this.generateOffsetMovedEvent(event);
                        String msg = "PULL_OFFSET_MOVED:correction offset. topic={}, groupId={}, requestOffset={}, newOffset={}, suggestBrokerId={}";
                        log.warn(msg, requestHeader.getTopic(), requestHeader.getConsumerGroup(), event.getOffsetRequest(), event.getOffsetNew(), responseHeader.getSuggestWhichBrokerId());
                    } else {
                        responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                        response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                        String msg = "PULL_OFFSET_MOVED:none correction. topic={}, groupId={}, requestOffset={}, suggestBrokerId={}";
                        log.warn(msg, requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueOffset(), responseHeader.getSuggestWhichBrokerId());
                    }

                    break;
                default:
                    assert false;
            }
        } else {

            // getMessageResult == null

            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store getMessage return null");
        }

        /*
         * 1.brokerAllowSuspend == true
         * 2.sysFlag 表示提交消费者本地该 queue 的 offset
         * 3.当前 broker 节点角色为 master 节点
         * 三个条件都成立的时候，才在 broker 端存储该消费者组内该 queue 的消费进度
         */
        boolean storeOffsetEnable = brokerAllowSuspend;
        storeOffsetEnable/*允许存储*/ = storeOffsetEnable && hasCommitOffsetFlag;
        storeOffsetEnable/*当前节点是master节点*/ = storeOffsetEnable && this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;
        if (storeOffsetEnable) {

            // 存储该消费者组下该主题的指定队列的消费进度
            ConsumerOffsetManager consumerOffsetManager = this.brokerController.getConsumerOffsetManager();
            consumerOffsetManager.commitOffset(RemotingHelper.parseChannelRemoteAddr(channel), requestHeader.getConsumerGroup(), requestHeader.getTopic(), queueIdInHeader, requestHeader.getCommitOffset());
        }

        // 返回，当 response 不为 null 的时候，外层 requestTask 的 callback 会将数据写回给客户端
        return response;
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    public void executeConsumeMessageHookBefore(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                    // ignore
                }
            }
        }
    }

    private byte[] readGetMessageResult(final GetMessageResult getMessageResult, final String group, final String topic, final int queueId) {

        // 根据 bufferTotalSize 创建大buffer
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize()/*本次查询消息的总大小*/);

        long storeTimestamp = 0;
        try {

            // 消息缓存列表
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {

                // 把 缓存 bb 的内容复制到 byteBuffer 中
                byteBuffer.put(bb/*消息都塞入*/);

                // 下面的逻辑先不管

                int sysFlag = bb.getInt(MessageDecoder.SYSFLAG_POSITION /* 4 + 4 + 4 + 4 + 4 + 8 + 8 */);
                //                bornhost has the IPv4 ip if the MessageSysFlag.BORNHOST_V6_FLAG bit of sysFlag is 0
                //                IPv4 host = ip(4 byte) + port(4 byte); IPv6 host = ip(16 byte) + port(4 byte)
                int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                int msgStoreTimePos/*消息存储时间的下标*/ = 4 // 1 TOTALSIZE
                        + 4 // 2 MAGICCODE
                        + 4 // 3 BODYCRC
                        + 4 // 4 QUEUEID
                        + 4 // 5 FLAG
                        + 8 // 6 QUEUEOFFSET
                        + 8 // 7 PHYSICALOFFSET
                        + 4 // 8 SYSFLAG
                        + 8 // 9 BORNTIMESTAMP
                        + bornhostLength; // 10 BORNHOST

                // 从下标位置开始读取8个字节，就是 消息存储时间 的值
                storeTimestamp = bb.getLong(msgStoreTimePos);
            }
        } finally {

            // 是否资源，结果都已经转移了！
            getMessageResult.release();
        }

        this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId, this.brokerController.getMessageStore().now() - storeTimestamp);
        return byteBuffer.array();
    }

    private void generateOffsetMovedEvent(final OffsetMovedEvent event) {
        try {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setTopic(MixAll.OFFSET_MOVED_EVENT /*OFFSET_MOVED_EVENT*/);
            msgInner.setTags(event.getConsumerGroup());
            msgInner.setDelayTimeLevel(0);
            msgInner.setKeys(event.getConsumerGroup());
            msgInner.setBody(event.encode());
            msgInner.setFlag(0);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(TopicFilterType.SINGLE_TAG, msgInner.getTags()));

            msgInner.setQueueId(0);
            msgInner.setSysFlag(0);
            msgInner.setBornTimestamp(System.currentTimeMillis());
            msgInner.setBornHost(RemotingUtil.string2SocketAddress(this.brokerController.getBrokerAddr()));
            msgInner.setStoreHost(msgInner.getBornHost());

            msgInner.setReconsumeTimes(0);
            this.brokerController.getMessageStore().putMessage(msgInner);
        } catch (Exception e) {
            log.warn(String.format("generateOffsetMovedEvent Exception, %s", event.toString()), e);
        }
    }

    public void executeRequestWhenWakeup(final Channel channel, final RemotingCommand request) {
        Runnable run = () -> {
            try {
                final RemotingCommand response = PullMessageProcessor.this.processRequest(channel, request, false /* 这里提交的任务不允许再次发起长轮询了，因为当前方法就是在长轮询中发起的 */);

                if (response != null) {
                    response.setOpaque(request.getOpaque());
                    response.markResponseType();
                    try {

                        // 发送结果给客户端
                        channel.writeAndFlush(response).addListener((ChannelFutureListener) future -> {
                            if (!future.isSuccess()) {
                                log.error("processRequestWrapper response to {} failed", future.channel().remoteAddress(), future.cause());
                                log.error(request.toString());
                                log.error(response.toString());
                            }
                        });
                    } catch (Throwable e) {
                        log.error("processRequestWrapper process request over, but response failed", e);
                        log.error(request.toString());
                        log.error(response.toString());
                    }
                }
            } catch (RemotingCommandException e1) {
                log.error("excuteRequestWhenWakeup run", e1);
            }
        };

        ExecutorService pullMessageExecutor = this.brokerController.getPullMessageExecutor();

        pullMessageExecutor.submit(new RequestTask(run, channel, request));
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> sendMessageHookList) {
        this.consumeMessageHookList = sendMessageHookList;
    }
}

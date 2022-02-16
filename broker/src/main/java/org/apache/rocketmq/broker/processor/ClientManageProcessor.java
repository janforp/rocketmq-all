package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.filter.FilterSpi;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.Set;

public class ClientManageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    public ClientManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * @param ctx 上下文
     * @param request 请求
     * @see BrokerController#registerProcessor()
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.HEART_BEAT:
                return this.heartBeat(ctx, request);
            case RequestCode.UNREGISTER_CLIENT:
                return this.unregisterClient(ctx, request);
            case RequestCode.CHECK_CLIENT_CONFIG:
                return this.checkClientConfig(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * @see MQClientInstance#sendHeartbeatToAllBroker() 生产者/消费者实例的心跳数据
     */
    public RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        // 解析得到请求包
        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(ctx.channel(), heartbeatData.getClientID(), request.getLanguage(), request.getVersion());

        // 消费者列表
        Set<ConsumerData> consumerDataSet = heartbeatData.getConsumerDataSet();
        for (ConsumerData data : consumerDataSet) {
            SubscriptionGroupManager subscriptionGroupManager = this.brokerController.getSubscriptionGroupManager();
            // 找到该 group 的订阅组配置
            SubscriptionGroupConfig subscriptionGroupConfig = subscriptionGroupManager.findSubscriptionGroupConfig(data.getGroupName());
            boolean isNotifyConsumerIdsChangedEnable = true;
            if (null != subscriptionGroupConfig) {
                isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable()/*默认 true*/;
                int topicSysFlag = 0;
                if (data.isUnitMode()) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                }
                // %RETRY% + consumerGroup
                String newTopic = MixAll.getRetryTopic(data.getGroupName());
                TopicConfigManager topicConfigManager = this.brokerController.getTopicConfigManager();
                topicConfigManager.createTopicInSendMessageBackMethod(newTopic, subscriptionGroupConfig.getRetryQueueNums(), PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
            }

            ConsumerManager consumerManager = this.brokerController.getConsumerManager();
            boolean changed = consumerManager
                    .registerConsumer(data.getGroupName(), clientChannelInfo, data.getConsumeType(), data.getMessageModel(), data.getConsumeFromWhere(), data.getSubscriptionDataSet(), isNotifyConsumerIdsChangedEnable);
            if (changed) {
                log.info("registerConsumer info changed {} {}", data.toString(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            }
        }

        // 生产者列表
        Set<ProducerData> producerDataSet = heartbeatData.getProducerDataSet();
        for (ProducerData data : producerDataSet) {
            ProducerManager producerManager = this.brokerController.getProducerManager();
            producerManager.registerProducer(data.getGroupName(), clientChannelInfo);
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 关闭某个生产者或者消费者的时候会调用该接口
     *
     * @see MQClientInstance#unregisterClient(java.lang.String, java.lang.String)
     */
    public RemotingCommand unregisterClient(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(UnregisterClientResponseHeader.class);
        final UnregisterClientRequestHeader requestHeader = (UnregisterClientRequestHeader) request.decodeCommandCustomHeader(UnregisterClientRequestHeader.class);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(ctx.channel(), requestHeader.getClientID(), request.getLanguage(), request.getVersion());

        {
            final String producerGroup = requestHeader.getProducerGroup();
            if (producerGroup != null) {
                ProducerManager producerManager = this.brokerController.getProducerManager();
                producerManager.unregisterProducer(producerGroup, clientChannelInfo);
            }
        }

        {
            final String consumerGroup = requestHeader.getConsumerGroup();
            if (consumerGroup != null) {
                SubscriptionGroupManager subscriptionGroupManager = this.brokerController.getSubscriptionGroupManager();
                SubscriptionGroupConfig subscriptionGroupConfig = subscriptionGroupManager.findSubscriptionGroupConfig(consumerGroup);
                boolean isNotifyConsumerIdsChangedEnable = true;
                if (null != subscriptionGroupConfig) {
                    isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                }
                ConsumerManager consumerManager = this.brokerController.getConsumerManager();
                consumerManager.unregisterConsumer(consumerGroup, clientChannelInfo, isNotifyConsumerIdsChangedEnable);
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 主要是校验当前订阅的主题所在的 broker 是否支持 表达式类型
     * 因为不是每个 broker 都支持 除 TAG 之外的过滤类型的
     */
    public RemotingCommand checkClientConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        CheckClientRequestBody requestBody = CheckClientRequestBody.decode(request.getBody(), CheckClientRequestBody.class);
        if (requestBody != null && requestBody.getSubscriptionData() != null) {

            // 订阅信息
            SubscriptionData subscriptionData = requestBody.getSubscriptionData();

            if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                // 如果是 tag 类型，直接成功
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            }

            // 不是 TAG 类型

            if (!this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
                // 配置是否开启
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
                return response;
            }

            // 开启了配置

            try {
                String expressionType = subscriptionData.getExpressionType();
                FilterFactory instance = FilterFactory.INSTANCE;
                FilterSpi filterSpi = instance.get(expressionType);
                filterSpi.compile(subscriptionData.getSubString());
            } catch (Exception e) {
                log.warn("Client {}@{} filter message, but failed to compile expression! sub={}, error={}", requestBody.getClientId(), requestBody.getGroup(), requestBody.getSubscriptionData(), e.getMessage());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark(e.getMessage());
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}

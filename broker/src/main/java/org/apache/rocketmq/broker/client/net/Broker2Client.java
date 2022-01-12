package org.apache.rocketmq.broker.client.net;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueForC;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBodyForC;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

@AllArgsConstructor
public class Broker2Client {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    public void checkProducerTransactionState(final String group, final Channel channel, final CheckTransactionStateRequestHeader requestHeader, final MessageExt messageExt) throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, requestHeader);
        request.setBody(MessageDecoder.encode(messageExt, false));

        RemotingServer remotingServer = this.brokerController.getRemotingServer();
        try {
            remotingServer.invokeOneway(channel, request, 10);
        } catch (Exception e) {
            log.error("Check transaction failed because invoke producer exception. group={}, msgId={}", group, messageExt.getMsgId(), e.getMessage());
        }
    }

    /**
     * broker订阅客户端（生产者或者消费者）
     *
     * @param channel 连接
     * @param request 请求
     * @return 响应
     */
    public RemotingCommand callClient(final Channel channel, final RemotingCommand request) throws RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingServer remotingServer = this.brokerController.getRemotingServer();
        return remotingServer.invokeSync(channel, request, 10000);
    }

    public void notifyConsumerIdsChanged(final Channel channel, final String consumerGroup) {
        if (null == consumerGroup) {
            log.error("notifyConsumerIdsChanged consumerGroup is null");
            return;
        }

        NotifyConsumerIdsChangedRequestHeader requestHeader = new NotifyConsumerIdsChangedRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, requestHeader);
        RemotingServer remotingServer = this.brokerController.getRemotingServer();
        try {
            remotingServer.invokeOneway(channel, request, 10);
        } catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception, " + consumerGroup, e.getMessage());
        }
    }

    public RemotingCommand resetOffset(String topic, String group, long timeStamp, boolean isForce) {
        return resetOffset(topic, group, timeStamp, isForce, false);
    }

    public RemotingCommand resetOffset(String topic, String group, long timeStamp, boolean isForce, boolean isC/*是否 C 语言*/) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        TopicConfigManager topicConfigManager = this.brokerController.getTopicConfigManager();
        // 获取该主题的配置
        TopicConfig topicConfig = topicConfigManager.selectTopicConfig(topic);
        if (null == topicConfig) {
            log.error("[reset-offset] reset offset failed, no topic in this broker. topic={}", topic);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("[reset-offset] reset offset failed, no topic in this broker. topic=" + topic);
            return response;
        }

        Map<MessageQueue, Long> offsetTable = new HashMap<>();

        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            MessageQueue mq = new MessageQueue();

            // broker-a
            String brokerName = this.brokerController.getBrokerConfig().getBrokerName();
            mq.setBrokerName(brokerName);
            mq.setTopic(topic);
            mq.setQueueId(i);

            ConsumerOffsetManager consumerOffsetManager = this.brokerController.getConsumerOffsetManager();
            long consumerOffset = consumerOffsetManager.queryOffset(group, topic, i);
            if (-1 == consumerOffset) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.format("THe consumer group <%s> not exist", group));
                return response;
            }

            long timeStampOffset;
            MessageStore messageStore = this.brokerController.getMessageStore();
            if (timeStamp == -1) {
                timeStampOffset = messageStore.getMaxOffsetInQueue(topic, i);
            } else {
                timeStampOffset = messageStore.getOffsetInQueueByTime(topic, i, timeStamp);
            }

            if (timeStampOffset < 0) {
                log.warn("reset offset is invalid. topic={}, queueId={}, timeStampOffset={}", topic, i, timeStampOffset);
                timeStampOffset = 0;
            }

            // 真正的重置 offset
            if (isForce || timeStampOffset < consumerOffset) {
                offsetTable.put(mq, timeStampOffset);
            } else {
                offsetTable.put(mq, consumerOffset);
            }
        }

        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timeStamp);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, requestHeader);
        if (isC) {
            // c++ language
            ResetOffsetBodyForC body = new ResetOffsetBodyForC();
            List<MessageQueueForC> offsetList = convertOffsetTable2OffsetList(offsetTable);
            body.setOffsetTable(offsetList);
            request.setBody(body.encode());
        } else {
            // other language
            ResetOffsetBody body = new ResetOffsetBody();
            body.setOffsetTable(offsetTable);
            request.setBody(body.encode());
        }

        // 拿到消费者组对应的信息
        ConsumerManager consumerManager = this.brokerController.getConsumerManager();
        ConsumerGroupInfo consumerGroupInfo = consumerManager.getConsumerGroupInfo(group);

        if (consumerGroupInfo != null && !consumerGroupInfo.getAllChannel().isEmpty()) {
            // 该消费者组与该 broker 建立了任何一个连接

            // 拿到所有的连接信息
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = consumerGroupInfo.getChannelInfoTable();
            for (Map.Entry<Channel, ClientChannelInfo> entry : channelInfoTable.entrySet()) {
                // 遍历每个连接信息

                int version = entry.getValue().getVersion();
                if (version >= MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                    try {
                        RemotingServer remotingServer = this.brokerController.getRemotingServer();
                        Channel channel = entry.getKey();
                        remotingServer.invokeOneway(channel, request, 5000);
                        log.info("[reset-offset] reset offset success. topic={}, group={}, clientId={}", topic, group, entry.getValue().getClientId());
                    } catch (Exception e) {
                        log.error("[reset-offset] reset offset exception. topic={}, group={}", new Object[] { topic, group }, e);
                    }
                } else {
                    // 低版本不支持该操作
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("the client does not support this feature. version=" + MQVersion.getVersionDesc(version));
                    log.warn("[reset-offset] the client does not support this feature. version={}", RemotingHelper.parseChannelRemoteAddr(entry.getKey()), MQVersion.getVersionDesc(version));
                    return response;
                }
            }
        } else {
            // 该消费者组没有与该 broker 建立任何连接，异常
            String errorInfo = String.format("Consumer not online, so can not reset offset, Group: %s Topic: %s Timestamp: %d", requestHeader.getGroup(), requestHeader.getTopic(), requestHeader.getTimestamp());
            log.error(errorInfo);
            response.setCode(ResponseCode.CONSUMER_NOT_ONLINE);
            response.setRemark(errorInfo);
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        ResetOffsetBody resBody = new ResetOffsetBody();
        resBody.setOffsetTable(offsetTable);
        response.setBody(resBody.encode());
        return response;
    }

    private List<MessageQueueForC> convertOffsetTable2OffsetList(Map<MessageQueue, Long> table) {
        List<MessageQueueForC> list = new ArrayList<>();
        for (Entry<MessageQueue, Long> entry : table.entrySet()) {
            MessageQueue mq = entry.getKey();
            MessageQueueForC tmp = new MessageQueueForC(mq.getTopic(), mq.getBrokerName(), mq.getQueueId(), entry.getValue());
            list.add(tmp);
        }
        return list;
    }

    public RemotingCommand getConsumeStatus(String topic, String group, String originClientId) {
        final RemotingCommand result = RemotingCommand.createResponseCommand(null);

        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, requestHeader);

        Map<String, Map<MessageQueue, Long>> consumerStatusTable = new HashMap<>();

        ConsumerManager consumerManager = this.brokerController.getConsumerManager();
        ConsumerGroupInfo consumerGroupInfo = consumerManager.getConsumerGroupInfo(group);
        // 该消费者组 group 与该 broker 建立的所有连接
        ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = consumerGroupInfo.getChannelInfoTable();
        if (null == channelInfoTable || channelInfoTable.isEmpty()) {
            // 没有任何连接
            result.setCode(ResponseCode.SYSTEM_ERROR);
            result.setRemark(String.format("No Any Consumer online in the consumer group: [%s]", group));
            return result;
        }

        // 遍历每个连接
        for (Map.Entry<Channel, ClientChannelInfo> entry : channelInfoTable.entrySet()) {
            int version = entry.getValue().getVersion();
            String clientId = entry.getValue().getClientId();
            if (version < MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                // 低版本不支持
                result.setCode(ResponseCode.SYSTEM_ERROR);
                result.setRemark("the client does not support this feature. version=" + MQVersion.getVersionDesc(version));
                log.warn("[get-consumer-status] the client does not support this feature. version={}", RemotingHelper.parseChannelRemoteAddr(entry.getKey()), MQVersion.getVersionDesc(version));
                return result;
            } else if (UtilAll.isBlank(originClientId) || originClientId.equals(clientId)) {
                try {
                    // 发起网络请求
                    RemotingCommand response = this.brokerController.getRemotingServer().invokeSync(entry.getKey(), request, 5000);
                    assert response != null;
                    if (response.getCode() == ResponseCode.SUCCESS) {
                        if (response.getBody() != null) {
                            GetConsumerStatusBody body = GetConsumerStatusBody.decode(response.getBody(), GetConsumerStatusBody.class);
                            Map<MessageQueue, Long> messageQueueTable = body.getMessageQueueTable();
                            consumerStatusTable.put(clientId, messageQueueTable);
                            log.info("[get-consumer-status] get consumer status success. topic={}, group={}, channelRemoteAddr={}", topic, group, clientId);
                        }
                    }
                } catch (Exception e) {
                    log.error("[get-consumer-status] get consumer status exception. topic={}, group={}, offset={}", new Object[] { topic, group }, e);
                }

                if (!UtilAll.isBlank(originClientId) && originClientId.equals(clientId)) {
                    break;
                }
            }
        }

        result.setCode(ResponseCode.SUCCESS);
        GetConsumerStatusBody resBody = new GetConsumerStatusBody();
        resBody.setConsumerTable(consumerStatusTable);
        result.setBody(resBody.encode());
        return result;
    }
}

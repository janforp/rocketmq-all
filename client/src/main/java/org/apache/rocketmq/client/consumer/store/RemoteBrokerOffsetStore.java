package org.apache.rocketmq.client.consumer.store;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Remote storage implementation
 */
public class RemoteBrokerOffsetStore implements OffsetStore {

    private final static InternalLogger log = ClientLogger.getLog();

    private final MQClientInstance mQClientFactory;

    private final String groupName;

    /**
     * 某个队列的消费进度映射
     * key:队列
     * value：偏移量
     */
    private final ConcurrentMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<MessageQueue, AtomicLong>();

    public RemoteBrokerOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
    }

    @Override
    public void load() {
    }

    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq == null) {
            return;
        }

        // 更新之前的偏移量
        AtomicLong offsetOld = this.offsetTable.get(mq);
        if (null == offsetOld) {
            offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
        }

        if (null != offsetOld) {
            if (increaseOnly) {
                MixAll.compareAndIncreaseOnly(offsetOld, offset);
            } else {
                offsetOld.set(offset);
            }
        }
    }

    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq == null) {
            return -1;
        }
        switch (type) {
            case MEMORY_FIRST_THEN_STORE:
            case READ_FROM_MEMORY: {
                AtomicLong offset = this.offsetTable.get(mq);
                if (offset != null) {
                    return offset.get();
                } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                    return -1;
                }
            }
            case READ_FROM_STORE: {
                try {
                    long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
                    AtomicLong offset = new AtomicLong(brokerOffset);
                    this.updateOffset(mq, offset.get(), false);
                    return brokerOffset;
                }
                // No offset in broker
                catch (MQBrokerException e) {
                    return -1;
                }
                //Other exceptions
                catch (Exception e) {
                    log.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                    return -2;
                }
            }
            default:
                break;
        }
        return -1;
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty()) {
            return;
        }

        final HashSet<MessageQueue> unusedMQ = new HashSet<MessageQueue>();

        // 遍历 ConcurrentMap<MessageQueue, AtomicLong> offsetTable
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            AtomicLong offset = entry.getValue();
            if (offset == null) {
                continue;
            }
            if (mqs.contains(mq)) {
                try {
                    this.updateConsumeOffsetToBroker(mq, offset.get());
                    log.info("[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}", this.groupName, this.mQClientFactory.getClientId(), mq, offset.get());
                } catch (Exception e) {
                    log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
                }
            } else {
                // 没有使用的 mq 队列
                unusedMQ.add(mq);
            }
        }

        if (!unusedMQ.isEmpty()) {
            for (MessageQueue mq : unusedMQ) {
                this.offsetTable.remove(mq);
                log.info("remove unused mq, {}, {}", mq, this.groupName);
            }
        }
    }

    @Override
    public void persist(MessageQueue mq) {
        AtomicLong offset = this.offsetTable.get(mq);
        if (offset != null) {
            try {
                this.updateConsumeOffsetToBroker(mq, offset.get());
                log.info("[persist] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}", this.groupName, this.mQClientFactory.getClientId(), mq, offset.get());
            } catch (Exception e) {
                log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
            }
        }
    }

    @Override
    public void removeOffset(MessageQueue mq) {
        if (mq != null) {
            this.offsetTable.remove(mq);
            log.info("remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}", this.groupName, mq, offsetTable.size());
        }
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());
        }
        return cloneOffsetTable;
    }

    /**
     * Update the Consumer Offset in one way, once the Master is off, updated to Slave, here need to be optimized.
     */
    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        updateConsumeOffsetToBroker(mq, offset, true);
    }

    /**
     * Update the Consumer Offset synchronously, once the Master is off, updated to Slave, here need to be optimized.
     */
    @Override
    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

        // 根据 brokerName 查询 broker
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }
        if (findBrokerResult == null) {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }

        UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
        requestHeader.setTopic(mq.getTopic());
        requestHeader.setConsumerGroup(this.groupName);
        requestHeader.setQueueId(mq.getQueueId());
        requestHeader.setCommitOffset(offset);

        MQClientAPIImpl mqClientAPIImpl = this.mQClientFactory.getMQClientAPIImpl();

        if (isOneway) {
            mqClientAPIImpl.updateConsumerOffsetOneway(findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
        } else {
            mqClientAPIImpl.updateConsumerOffset(findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
        }
    }

    private long fetchConsumeOffsetFromBroker(MessageQueue mq) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        // 拿到的 broker 信息
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {

            // 如果拿不到，可能需要更新
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            // 更新之后再拿一次
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult == null) {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }

        QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
        requestHeader.setTopic(mq.getTopic());
        requestHeader.setConsumerGroup(this.groupName);
        requestHeader.setQueueId(mq.getQueueId());

        MQClientAPIImpl mqClientAPIImpl = this.mQClientFactory.getMQClientAPIImpl();
        // 真正的发送网络请求取查询偏移量
        return mqClientAPIImpl.queryConsumerOffset(findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
    }
}

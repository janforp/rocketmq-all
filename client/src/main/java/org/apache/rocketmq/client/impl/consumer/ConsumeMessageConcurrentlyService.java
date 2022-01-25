package org.apache.rocketmq.client.impl.consumer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {

    private static final InternalLogger log = ClientLogger.getLog();

    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    // 门面。充当配置的作用
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    // 消费监听器，消息的处理逻辑在这里封装，由开发者实现，并且注册到 defaultMQPushConsumer中
    private final MessageListenerConcurrently messageListener;

    // 消费任务线程池
    private final ThreadPoolExecutor consumeExecutor;

    // 消费者组
    private final String consumerGroup;

    // 调度线程池：延迟提交消费任务
    private final ScheduledExecutorService scheduledExecutorService;

    // 调度线程池：清理过期消息
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl, MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();

        // 消费任务队列
        BlockingQueue<Runnable> consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        this.consumeExecutor = new ThreadPoolExecutor(
                this.defaultMQPushConsumer.getConsumeThreadMin()/*20*/, this.defaultMQPushConsumer.getConsumeThreadMax()/*20*/, 1000 * 60, TimeUnit.MILLISECONDS, consumeRequestQueue, new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
    }

    @Override
    public void start() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                cleanExpireMsg();
            }

        }, this.defaultMQPushConsumer.getConsumeTimeout() /*15分钟*/, this.defaultMQPushConsumer.getConsumeTimeout()/*每15分钟执行一次*/, TimeUnit.MINUTES);
    }

    @Override
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.consumeExecutor.shutdown();
        this.cleanExpireMsgExecutors.shutdown();
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0 && corePoolSize <= Short.MAX_VALUE && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s", RemotingHelper.exceptionSimpleDesc(e), ConsumeMessageConcurrentlyService.this.consumerGroup, msgs, mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    /**
     * @param msgs 待消费的消息
     * @param processQueue 本地队列快照，提交消费任务之前，msgs已经加入到该pq内了
     * @param messageQueue 队列
     * @param dispatchToConsume 并发消费不关注该参数，这些消息是否以及保存到拉快照中
     * @see DefaultMQPushConsumerImpl#pullMessage(org.apache.rocketmq.client.impl.consumer.PullRequest) 该方法的 pullCallback 会调用该方法
     */
    @Override
    public void submitConsumeRequest(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue, final boolean dispatchToConsume/*没有用到*/) {
        // 每次消费消息的最大数量（默认是1）
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        if (msgs.size() <= consumeBatchSize) {
            // 如果本次拉到的消息总数还没有超过最大数量，则一次性全部提交

            // 批量消费
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }
        } else {
            // 本次消息数量大于 最大数量，则分批提交

            // 遍历消息，然后消费
            for (int total = 0; total < msgs.size(); ) {

                // 按要求的批次消费
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    // 线程池拒绝了新提交的任务，则延迟 一会再次提交消费 任务
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }
                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }

    private void cleanExpireMsg() {
        RebalanceImpl rebalanceImpl = this.defaultMQPushConsumerImpl.getRebalanceImpl();

        // processQueueTable 存储的是分配给当前消费者队列（mq,pd）
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = rebalanceImpl.getProcessQueueTable();

        for (Map.Entry<MessageQueue, ProcessQueue> next : processQueueTable.entrySet()) {
            // 待处理队列
            ProcessQueue pq = next.getValue();
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    /**
     * 消费完之后，处理消费结果
     * 如果消费成功更新pd（删除消费成功的消息，或者 在消费失败的是进行消息回退，并且将回退失败的消息再次提交到消费任务中，最后也会将msgs从pd移除，最后更新消费进度）
     *
     * @param status 消费结果状态
     * @param context 上下文
     * @param consumeRequest 消费任务本身
     */
    public void processConsumeResult(final ConsumeConcurrentlyStatus status, final ConsumeConcurrentlyContext context, final ConsumeRequest consumeRequest) {

        // 回退消息开始下标
        int ackIndex = context.getAckIndex();

        if (consumeRequest.getMsgs().isEmpty()) {
            return;
        }

        switch (status) {
            case CONSUME_SUCCESS: // 消费成功
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    // 消息数量- 1 ，如：32 -1 = 31
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }

                // 监控统计相关
                int ok = ackIndex + 1;
                int failed = consumeRequest.getMsgs().size() - ok;
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                // 监控统计相关

                break;
            case RECONSUME_LATER: // 消费失败
                // 改成-1，回退所有消息
                ackIndex = -1;
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
                for (int i = ackIndex + 1/*失败，则从0开始遍历，成功则从 31 开始遍历*/; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            case CLUSTERING:

                // 回退失败的消息列表
                List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
                for (int i = ackIndex + 1/*失败，则从0开始遍历，成功则从 31 开始遍历*/; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    // 回退消息
                    boolean result = this.sendMessageBack(msg, context);
                    if (!result) {

                        // 将消息重试属性 + 1
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        // 加入到回退失败集合
                        msgBackFailed.add(msg);
                    }
                }

                if (!msgBackFailed.isEmpty()) {
                    // 将回退失败的消息从当前消费任务中移除
                    consumeRequest.getMsgs().removeAll(msgBackFailed);
                    // 回退失败的消息延迟 5 s 之后再次提交到消费任务中
                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }

        // 从pd 中删除消费成功的消息，返回 -1说明pd没数据，queueOffsetMax + 1：说明删除这一批之后没消息了，msgTreeMap.firstKey()：删除完这一批之后还有消息
        ProcessQueue processQueue = consumeRequest.getProcessQueue();
        long offset = processQueue.removeMessage(consumeRequest.getMsgs());
        if (offset >= 0 && !processQueue.isDropped()) {
            // 更新消费进度
            OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
            MessageQueue messageQueue = consumeRequest.getMessageQueue();
            offsetStore.updateOffset(messageQueue, offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    /**
     * 消息回退
     *
     * @param msg 消息
     * @param context 上下文
     * @return 成功失败
     */
    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // Wrap topic with namespace before sending back message.
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        try {
            String brokerName = context.getMessageQueue().getBrokerName();
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg, e);
        }

        return false;
    }

    private void submitConsumeRequestLater(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue) {
        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest) {
        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    @AllArgsConstructor
    class ConsumeRequest implements Runnable {

        @Getter        // 消息
        private final List<MessageExt> msgs;

        @Getter
        private final ProcessQueue processQueue;

        @Getter
        private final MessageQueue messageQueue;

        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                // 该队列可能被负载均衡到其他节点了，该消费者实例不需要消费该消息；
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            // 消费上下文
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);

            // 消费成功失败
            ConsumeConcurrentlyStatus status = null;

            // 对 msgs 中的每条消息的 topic 属性进行一些操作
            // 主要是把重复消费的消息的主题设置为原主题
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);

                // 执行钩子
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            long beginTimestamp = System.currentTimeMillis();

            // 用户实现的监听器是否抛出异常
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                if (!msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        // 设置开始消费的时间到消息属性，为什么要设置呢？因为清理过期消息的任务会检查这个属性，根据该值来判断是否过期
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }

                @SuppressWarnings("all")
                MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
                // 使用消息监听器消费消息，这个监听器一般是用户自己的业务逻辑
                status/*消费成功/失败重试*/ = listener.consumeMessage(Collections.unmodifiableList(msgs) /* 不可写的list */, context /* 主要是在消息消息失败的时候，控制延迟级别 */);
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}", RemotingHelper.exceptionSimpleDesc(e), ConsumeMessageConcurrentlyService.this.consumerGroup, msgs, messageQueue);

                // 标记发生了异常
                hasException = true;
            }
            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            if (null == status) {
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                assert consumeMessageContext != null;
                // 钩子
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}", ConsumeMessageConcurrentlyService.this.consumerGroup, msgs, messageQueue);
                // 消费失败，重复消费
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                assert consumeMessageContext != null;
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager().incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            if (!processQueue.isDropped()) {
                // 消费完之后，处理消费结果
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this /* 包含了消息以及队列 */);
            } else {
                // 执行到这里，说明 listener 执行的时候，该 mq 被分配到其他 消费者 或者 消费者退出了
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }
    }
}

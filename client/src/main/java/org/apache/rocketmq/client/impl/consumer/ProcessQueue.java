package org.apache.rocketmq.client.impl.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Queue consumption snapshot
 *
 * 可以理解成服务器端MessageQueue在消费者端的一个快照！！！
 * 拉下来的消息先放到这里
 * 消费完之后从这里移除
 */
public class ProcessQueue {

    // 最长的加锁时间
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));

    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));

    // 拉消息的最大空闲时间
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));

    private final InternalLogger log = ClientLogger.getLog();

    // 读写锁
    private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();

    /**
     * 存储消息的容器,内部是按照 key 排序的
     *
     * @see ProcessQueue#lockTreeMap 需要获取这个锁才能操作该map
     * 从 broker 拉取到的消息先存储在这里
     */
    @Getter
    private final TreeMap<Long/*TODO 是逻辑偏移量呢还是物理偏移量？*/, MessageExt/*消息对象*/> msgTreeMap = new TreeMap<Long, MessageExt>();

    /**
     * 当前对象存储消息的条数
     * 消息数量
     */
    @Getter
    private final AtomicLong msgCount = new AtomicLong();

    /**
     * 当前队列存储的消息的总大小
     * 消息体总大小
     */
    @Getter
    private final AtomicLong msgSize = new AtomicLong();

    // 重入锁，顺序消费的时候使用
    @Getter
    private final Lock lockConsume = new ReentrantLock();

    /**
     * A subset of msgTreeMap, will only be used when orderly consume
     * 顺序消费临时容器
     */
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();

    private final AtomicLong tryUnlockTimes = new AtomicLong(0);

    /**
     * 快照中的最大的 offset
     */
    private volatile long queueOffsetMax = 0L;

    /**
     * 是否移除，负载均衡的时候会修改这个字段
     * 消费的时候每次都会检查这个状态，及时知道负载均衡的变化
     */
    @Setter
    @Getter
    private volatile boolean dropped = false;

    /**
     * 上次拉消息的时间
     *
     * @see DefaultMQPushConsumerImpl#pullMessage(org.apache.rocketmq.client.impl.consumer.PullRequest) processQueue.setLastPullTimestamp(System.currentTimeMillis());
     */
    @Setter
    @Getter
    private volatile long lastPullTimestamp = System.currentTimeMillis();

    // 上次消费时间
    @Setter
    @Getter
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();

    /**
     * 锁定状态
     *
     * @see RebalanceImpl#lock(org.apache.rocketmq.common.message.MessageQueue)
     */
    @Setter
    @Getter
    private volatile boolean locked = false;

    /**
     * 上次获取锁的时间
     */
    @Setter
    @Getter
    private volatile long lastLockTimestamp = System.currentTimeMillis();

    // 是否消费中
    private volatile boolean consuming = false;

    @Setter
    @Getter
    private volatile long msgAccCnt = 0;

    public boolean isLockExpired() {
        // 锁是否过期？
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    public boolean isPullExpired() {
        // 拉消息请求是否过期
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME /* 120 s */;
    }

    @SuppressWarnings("all")
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            // 顺序消费不做清理逻辑
            return;
        }

        // 做多循环16次
        int loop = Math.min(msgTreeMap.size(), 16);
        for (int i = 0; i < loop; i++) {
            MessageExt msg = null;
            try {
                this.lockTreeMap.readLock().lockInterruptibly();
                try {
                    if (!msgTreeMap.isEmpty() && System.currentTimeMillis() - Long.parseLong(MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry()/*第一条消息*/.getValue())) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                        // 消息不为空并且第一条消息已经过期（消费开始时间与系统时间的差超过15分钟了），则进入这里
                        msg = msgTreeMap.firstEntry().getValue();
                    } else {

                        // 第一条消息没有过期，则后面的就不看了
                        // 为什么可以跳出去呢？第一条不过期，则后面的肯定都没有过期
                        break;
                    }
                } finally {
                    this.lockTreeMap.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            try {
                // 消息回退到服务器，该消息的延迟级别为 3
                pushConsumer.sendMessageBack(msg /* 过期消息 */, 3 /*  延迟级别 */);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                try {
                    this.lockTreeMap.writeLock().lockInterruptibly();
                    try {
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey() /* 成立：该目标消息并没有被消费任务成功消费 ，如果不成立：则该消息被成功消费了，消费成功后执行的 processConsumeResult 会将目标消息从 msgTreeMap 中移除，导致该条件不成立*/) {
                            try {
                                // 回退成功，从 treeMsgMap 中删除该消息
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        this.lockTreeMap.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }

    /**
     * 将从 broker 拉取下来的消息 messageExtList 存储到快照队列内
     *
     * @param messageExtList broker 拉取下来的消息
     * @return 顺序消费的时候依赖该返回值(true : 提交顺序消费任务 ， false ： 不提交顺序消费任务), 并发消费服务无视该返回值
     */
    public boolean putMessage(final List<MessageExt> messageExtList) {
        boolean dispatchToConsume = false;
        try {

            // 获取锁
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                // 有效消息数量
                int validMsgCnt = 0;

                /*
                 * 将拉取到的消息全部加入到 msgTreeMap 中
                 * 更新字段
                 */
                for (MessageExt msg : messageExtList) {
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    if (null == old) {
                        // 加入了一条新消息，则有效消息数量加一
                        validMsgCnt++;
                        this.queueOffsetMax = msg.getQueueOffset();
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }
                msgCount.addAndGet(validMsgCnt);

                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    /*
                     * msgTreeMap 中存在未处理消息并且 当前不是正在消费中的时候
                     * 设置返回值为 true(需要提交消费任务)
                     * 设置consuming 为 ture 表示线上消费消息任务执行中
                     */
                    dispatchToConsume = true;
                    this.consuming = true;
                }

                if (!messageExtList.isEmpty()) {
                    // 拿到最后一条消息
                    MessageExt messageExt = messageExtList.get(messageExtList.size() - 1);
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if (property != null) {
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }

        return dispatchToConsume;
    }

    public long getMaxSpan() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    // 计数第一条消息跟最后一条消息的偏移量差
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    /**
     * 移除已经消费的消息，本方法为并发消费服务使用
     *
     * @param messageExtList 已经消费过的消息
     * @return 偏移量（msgTreeMap.firstKey 或者 queueMaxOffset + 1）:-1说明pd没数据，queueOffsetMax + 1：说明删除这一批之后没消息了，msgTreeMap.firstKey()：删除完这一批之后还有消息
     */
    public long removeMessage(final List<MessageExt> messageExtList) {
        long result = -1;
        final long now = System.currentTimeMillis();
        try {
            // 获取锁
            this.lockTreeMap.writeLock().lockInterruptibly();
            // 设置最后一次消费的时间
            this.lastConsumeTimestamp = now;
            try {
                /*
                 * 将已经消费的消息全部移除
                 * 并且更新字段
                 */
                if (!msgTreeMap.isEmpty()) {
                    // 快照中的最大的 offset + 1
                    result = this.queueOffsetMax + 1;
                    int removedCnt = 0;
                    for (MessageExt msg : messageExtList) {
                        long queueOffset = msg.getQueueOffset();
                        MessageExt prev = msgTreeMap.remove(queueOffset);
                        if (prev != null) {
                            removedCnt--;
                            msgSize.addAndGet(-msg.getBody().length);
                        }
                    }
                    msgCount.addAndGet(removedCnt);

                    if (!msgTreeMap.isEmpty()) {
                        result = msgTreeMap.firstKey();
                    }
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }
        // 返回偏移量
        return result;
    }

    public void rollback() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    /**
     * 会将本次处理的消息从临时map中移除，并且返回消费进度
     * 顺序消费任务使用的方法
     * 处理完一批消息后回调用
     *
     * @see ConsumeMessageOrderlyService#processConsumeResult(java.util.List, org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus, org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext, org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService.ConsumeRequest)
     */
    public long commit() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
                msgCount.addAndGet(-this.consumingMsgOrderlyTreeMap.size());
                for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                    msgSize.addAndGet(-msg.getBody().length);
                }
                this.consumingMsgOrderlyTreeMap.clear();
                if (offset != null) {
                    return offset + 1;
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("commit exception", e);
        }

        return -1;
    }

    public void makeMessageToConsumeAgain(List<MessageExt> msgs) {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    /**
     * 获取一批次消息（顺序消费任务使用该方法获取消息）
     *
     * @param batchSize 本次提取消息的数量
     * @return 一批消息
     */
    public List<MessageExt> takeMessags(final int batchSize) {
        List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {

                    /*
                     * 从 msgTreeMap 的头节点开始提取 batchSize 个消息
                     * 1.提取的消息存储到 consumingMsgOrderlyTreeMap 内
                     * 2.消息加入 result 内
                     */
                    for (int i = 0; i < batchSize; i++) {
                        // 从主容器中拿出来哦
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            // 放入结果表
                            result.add(entry.getValue());
                            // 顺序消息先放入这里
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                        } else {
                            // 队列空了，则跳出循环
                            break;
                        }
                    }
                }

                if (result.isEmpty()) {
                    // 没有可以消费的消息了，则该队列对应的消费任务可以暂时停止了
                    consuming = false;
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    public boolean hasTempMessage() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
            // ignore
        }

        return true;
    }

    public void clear() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
            // ignore
        } finally {
            this.lockTreeMap.readLock().unlock();
        }
    }
}

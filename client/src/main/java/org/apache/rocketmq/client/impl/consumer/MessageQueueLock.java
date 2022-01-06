package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Message lock,strictly ensure the single queue only one thread at a time consuming
 */
public class MessageQueueLock {

    /**
     * 为了保证在消费者本地一个队列只有一个消费任务
     */
    private final ConcurrentMap<MessageQueue, Object> mqLockTable = new ConcurrentHashMap<MessageQueue, Object>();

    /**
     * 为什么要获取objLock锁？我们不是已经获取了 Broker 端该 队列 的独占锁了吗？
     * 原因：
     * broker queue 占用锁的角度是 client 占用， client 从 broker 的某个占用了锁的 queue 拉取下来消息之后，将消息存储到了消息本地的 processQueue 中，processQueue 快照对象内有个属性叫做 volatile boolean consuming,表示本地的队列是否正在被消费
     * 处理中。processQueue.takeMessage（）方法，到 processQueue 获取下一批次 待处理消息 获取到了的话 run （） 方法就处理，获取不到就会修改 processQueue.consuming = false，本消费任务马上终止了，但似乎从代码的角度去看还有一些代码要跑，
     * 此时，如果恰巧 pull 再次拉回来一批当前 processQueue 的 msg，会再次向 ConsumeMessageOrderlyService 提交消费任务，所以需要获取锁
     *
     * @param mq 队列
     * @return 锁
     */
    public Object fetchLockObject(final MessageQueue mq) {
        Object objLock = this.mqLockTable.get(mq);
        if (null == objLock) {
            objLock = new Object();
            Object prevLock = this.mqLockTable.putIfAbsent(mq, objLock);
            if (prevLock != null) {
                objLock = prevLock;
            }
        }
        return objLock;
    }
}
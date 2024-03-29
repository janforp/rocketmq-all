package org.apache.rocketmq.common.consumer;

public enum ConsumeFromWhere {

    /**
     * 从队列的最后 offset 开始消费，意思就是当队列内再有新消息加入的时候才开始消费
     */
    CONSUME_FROM_LAST_OFFSET,

    /**
     * 从有效 offset 开始消费
     */
    CONSUME_FROM_FIRST_OFFSET,

    /**
     * 从指定时间最近的消息开始消费
     *
     * @see org.apache.rocketmq.client.consumer.DefaultMQPushConsumer#consumeTimestamp 二者配合使用！！
     * @see org.apache.rocketmq.client.impl.consumer.RebalancePushImpl#computePullFromWhere(org.apache.rocketmq.common.message.MessageQueue)
     * @see org.apache.rocketmq.broker.processor.AdminBrokerProcessor#searchOffsetByTimestamp(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)
     */
    CONSUME_FROM_TIMESTAMP,

    /****/

    @Deprecated
    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
    @Deprecated
    CONSUME_FROM_MIN_OFFSET,
    @Deprecated
    CONSUME_FROM_MAX_OFFSET,
}
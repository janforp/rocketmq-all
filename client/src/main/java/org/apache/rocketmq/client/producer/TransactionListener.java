package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public interface TransactionListener {

    /**
     * When send transactional prepare(half) message succeed, this method will be invoked to execute local transaction.
     *
     * @param msg Half(prepare) message
     * @param arg Custom business parameter
     * @return Transaction state
     */
    LocalTransactionState executeLocalTransaction(final Message msg, final Object arg /* 业务层的参数 */);

    /**
     * 查询该半消息对应事务的状态,
     * 该方法由客户端实例调用！！！！！
     *
     * When no response to prepare(half) message. broker will send check message to check the transaction status, and this
     * method will be invoked to get local transaction status.
     *
     * @param msg Check message
     * @return Transaction state 查询该半消息对应事务的状态
     * @see DefaultMQProducerImpl#checkTransactionState(java.lang.String, org.apache.rocketmq.common.message.MessageExt, org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader)
     */
    LocalTransactionState checkLocalTransaction(final MessageExt msg);
}
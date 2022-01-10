package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.common.message.MessageExt;

/**
 * This interface will be removed in the version 5.0.0, interface {@link TransactionListener} is recommended.
 */
@Deprecated
public interface TransactionCheckListener {

    /**
     * 回查
     *
     * 查询该半消息对应事务的状态
     *
     * @param msg 半消息
     * @return 查询该半消息对应事务的状态
     */
    LocalTransactionState checkLocalTransactionState(final MessageExt msg);
}
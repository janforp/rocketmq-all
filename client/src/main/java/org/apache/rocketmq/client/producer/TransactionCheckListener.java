package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.common.message.MessageExt;

/**
 * This interface will be removed in the version 5.0.0, interface {@link TransactionListener} is recommended.
 */
@Deprecated
public interface TransactionCheckListener {

    LocalTransactionState checkLocalTransactionState(final MessageExt msg);
}

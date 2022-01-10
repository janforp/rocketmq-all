package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.common.message.Message;

/**
 * This interface will be removed in the version 5.0.0, interface {@link TransactionListener} is recommended.
 */
@Deprecated
public interface LocalTransactionExecuter {

    LocalTransactionState executeLocalTransactionBranch(final Message msg, final Object arg);
}
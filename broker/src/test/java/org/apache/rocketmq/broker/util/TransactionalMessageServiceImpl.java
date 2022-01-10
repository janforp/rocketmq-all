package org.apache.rocketmq.broker.util;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;

import java.util.concurrent.CompletableFuture;

public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return null;
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner) {
        return null;
    }

    @Override
    public boolean deletePrepareMessage(MessageExt messageExt) {
        return false;
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return null;
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return null;
    }

    @Override
    public void check(long transactionTimeout, int transactionCheckMax, AbstractTransactionalMessageCheckListener listener) {
        log.warn("check check!");
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }
}

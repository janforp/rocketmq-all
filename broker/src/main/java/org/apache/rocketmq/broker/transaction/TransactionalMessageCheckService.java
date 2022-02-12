package org.apache.rocketmq.broker.transaction;

import lombok.AllArgsConstructor;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.transaction.queue.DefaultTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 二阶段提交失败的事务消息，通过该任务定时回查事务消息的事务状态！
 */
@SuppressWarnings("all")
@AllArgsConstructor
public class TransactionalMessageCheckService extends ServiceThread {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private final BrokerController brokerController;

    @Override
    public void run() {
        log.info("Start transaction check service thread!");
        long checkInterval = brokerController.getBrokerConfig().getTransactionCheckInterval();/*60s*/
        while (!this.isStopped()) {
            this.waitForRunning(checkInterval);
        }
        log.info("End transaction check service thread!");
    }

    @Override
    protected void onWaitEnd() {
        long timeout/*6秒*/ = brokerController.getBrokerConfig().getTransactionTimeOut();
        int checkMax/*15次*/ = brokerController.getBrokerConfig().getTransactionCheckMax();// 回查最大次数
        long begin = System.currentTimeMillis();
        log.info("Begin to check prepare message, begin time:{}", begin);
        // 妈的 写在这里，说明意思
        /**
         * @see TransactionalMessageServiceImpl
         */
        TransactionalMessageService service = this.brokerController.getTransactionalMessageService();
        /**
         * @see DefaultTransactionalMessageCheckListener
         */
        AbstractTransactionalMessageCheckListener listener = this.brokerController.getTransactionalMessageCheckListener(); // 回查监听器
        /**
         * @see TransactionalMessageServiceImpl#check(long, int, AbstractTransactionalMessageCheckListener)
         */
        service.check(timeout, checkMax, listener);
        log.info("End to check prepare message, consumed time:{}", System.currentTimeMillis() - begin);
    }
}
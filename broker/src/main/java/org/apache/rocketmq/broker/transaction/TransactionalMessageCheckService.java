package org.apache.rocketmq.broker.transaction;

import lombok.AllArgsConstructor;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

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
        long timeout = brokerController.getBrokerConfig().getTransactionTimeOut();
        int checkMax = brokerController.getBrokerConfig().getTransactionCheckMax();// 回查最大次数
        long begin = System.currentTimeMillis();
        log.info("Begin to check prepare message, begin time:{}", begin);

        // 妈的 写在这里，说明意思
        TransactionalMessageService service = this.brokerController.getTransactionalMessageService();
        AbstractTransactionalMessageCheckListener listener = this.brokerController.getTransactionalMessageCheckListener(); // 回查监听器
        service.check(timeout, checkMax, listener);
        log.info("End to check prepare message, consumed time:{}", System.currentTimeMillis() - begin);
    }
}
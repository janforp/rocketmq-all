package org.apache.rocketmq.broker.latency;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.apache.rocketmq.store.MessageStore;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BrokerFastFailure {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("BrokerFastFailureScheduledThread"));

    private final BrokerController brokerController;

    public BrokerFastFailure(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    // 类型强转
    public static RequestTask castRunnable(final Runnable runnable) {
        try {
            if (runnable instanceof FutureTaskExt) {
                FutureTaskExt object = (FutureTaskExt) runnable;
                return (RequestTask) object.getRunnable();
            }
        } catch (Throwable e) {
            log.error(String.format("castRunnable exception, %s", runnable.getClass().getName()), e);
        }

        return null;
    }

    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            if (brokerController.getBrokerConfig().isBrokerFastFailureEnable()/*当前broker是否开启了快速失败功能*/) {
                cleanExpiredRequest();
            }
        }, 1000, 10, TimeUnit.MILLISECONDS);
    }

    private void cleanExpiredRequest() {
        MessageStore messageStore = this.brokerController.getMessageStore();
        while (messageStore.isOSPageCacheBusy() /*系统缓存是否繁忙？*/) {
            try {
                if (!this.brokerController.getSendThreadPoolQueue().isEmpty() /*任务队列还有任务*/) {
                    // 拿到一个任务
                    final Runnable runnable = this.brokerController.getSendThreadPoolQueue().poll(0, TimeUnit.SECONDS);
                    if (null == runnable) {
                        break;
                    }

                    final RequestTask rt = castRunnable(runnable);
                    String msg = "[PCBUSY_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d";

                    // 任务直接设置为失败结果，返回
                    rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format(msg, System.currentTimeMillis() - rt.getCreateTimestamp(), this.brokerController.getSendThreadPoolQueue().size()));
                } else {

                    // 任务队列中没有任务了
                    break;
                }
            } catch (Throwable ignored) {
            }
        }

        cleanExpiredRequestInQueue(this.brokerController.getSendThreadPoolQueue(), this.brokerController.getBrokerConfig().getWaitTimeMillsInSendQueue());

        cleanExpiredRequestInQueue(this.brokerController.getPullThreadPoolQueue(), this.brokerController.getBrokerConfig().getWaitTimeMillsInPullQueue());

        cleanExpiredRequestInQueue(this.brokerController.getHeartbeatThreadPoolQueue(), this.brokerController.getBrokerConfig().getWaitTimeMillsInHeartbeatQueue());

        cleanExpiredRequestInQueue(this.brokerController.getEndTransactionThreadPoolQueue(), this.brokerController.getBrokerConfig().getWaitTimeMillsInTransactionQueue());
    }

    void cleanExpiredRequestInQueue(final BlockingQueue<Runnable> blockingQueue, final long maxWaitTimeMillsInQueue) {
        String timeout = "[TIMEOUT_CLEAN_QUEUE]broker busy, start flow control for a while, period in queue: %sms, size of queue: %d";
        while (true) {
            try {
                if (!blockingQueue.isEmpty()) {
                    final Runnable runnable = blockingQueue.peek();
                    if (null == runnable) {
                        break;
                    }
                    final RequestTask rt = castRunnable(runnable);
                    if (rt == null || rt.isStopRun()) {
                        break;
                    }

                    final long behind = System.currentTimeMillis() - rt.getCreateTimestamp();
                    if (behind >= maxWaitTimeMillsInQueue) {
                        // 超时了
                        if (blockingQueue.remove(runnable)) {
                            rt.setStopRun(true);
                            // 任务直接设置为失败结果，返回
                            rt.returnResponse(RemotingSysResponseCode.SYSTEM_BUSY, String.format(timeout, behind, blockingQueue.size()));
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } catch (Throwable ignored) {
            }
        }
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }
}

package org.apache.rocketmq.client.impl.consumer;

import lombok.Getter;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @see DefaultMQPushConsumerImpl#pullMessage(org.apache.rocketmq.client.impl.consumer.PullRequest)
 */
public class PullMessageService extends ServiceThread {

    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 存储请求的阻塞队列
     */
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<PullRequest>();

    /**
     * 代理执行任务的对象
     */
    private final MQClientInstance mQClientFactory;

    @Getter
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "PullMessageServiceScheduledThread");
        }
    });

    public PullMessageService(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    // 延迟一段时间之后再拉消息
    public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        if (isStopped()) {
            // 如果已经停止，则日志
            log.warn("PullMessageServiceScheduledThread has shutdown");
            return;
        }
        // 如果当前任务还没停止，则执行
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                /*
                 *  并不是在一定时间之后执行，而是在一定时间之后把请求塞入阻塞队列
                 *  任务进去队列之后，由长轮训的 run 方法执行任务
                 *  @see PullMessageService#run()
                 */
                PullMessageService.this.executePullRequestImmediately(pullRequest);
            }

            // 延迟一段时间之后再执行
        }, timeDelay, TimeUnit.MILLISECONDS);
    }

    public void executePullRequestImmediately(final PullRequest pullRequest) {
        try {
            /*
             *  并不是在一定时间之后执行，而是在一定时间之后把请求塞入阻塞队列
             *  任务进去队列之后，由长轮训的 run 方法执行任务
             *  @see PullMessageService#run()
             */
            this.pullRequestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            log.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    /**
     * 真正的去拉数据，执行请求
     *
     * @param pullRequest 请求
     * @see DefaultMQPushConsumerImpl#pullMessage(org.apache.rocketmq.client.impl.consumer.PullRequest)
     */
    private void pullMessage(final PullRequest pullRequest) {
        String consumerGroup = pullRequest.getConsumerGroup();
        // 拿到该组的消费者
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(consumerGroup);
        if (consumer == null) {
            log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
            return;
        }
        // 调用消费者的拉消息方法
        DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
        impl.pullMessage(pullRequest);
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                // 没有请求，则阻塞在这里
                PullRequest pullRequest = this.pullRequestQueue.take();
                this.pullMessage(pullRequest);
            } catch (InterruptedException ignored) {
                // ignore
            } catch (Exception e) {
                log.error("Pull Message Service Run Method exception", e);
            }
        }
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }
}

package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 定时扫描
 * 1.生产者
 * 2.消费者
 * 3.过滤器
 */
public class ClientHousekeepingService implements ChannelEventListener {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ClientHousekeepingScheduledThread"));

    public ClientHousekeepingService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                ClientHousekeepingService.this.scanExceptionChannel();
            } catch (Throwable e) {
                log.error("Error occurred when scan not active client channels.", e);
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
    }

    private void scanExceptionChannel() {

        // 扫描生产者
        ProducerManager producerManager = this.brokerController.getProducerManager();
        producerManager.scanNotActiveChannel();

        // 扫描消费者
        ConsumerManager consumerManager = this.brokerController.getConsumerManager();
        consumerManager.scanNotActiveChannel();

        // 扫描过滤器
        this.brokerController.getFilterServerManager().scanNotActiveChannel();
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {

    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        // 关闭生产者
        ProducerManager producerManager = this.brokerController.getProducerManager();
        producerManager.doChannelCloseEvent(remoteAddr, channel);

        // 关闭消费者
        ConsumerManager consumerManager = this.brokerController.getConsumerManager();
        consumerManager.doChannelCloseEvent(remoteAddr, channel);

        // 关闭过滤器
        this.brokerController.getFilterServerManager().doChannelCloseEvent(remoteAddr, channel);
    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        // 关闭生产者
        ProducerManager producerManager = this.brokerController.getProducerManager();
        producerManager.doChannelCloseEvent(remoteAddr, channel);

        // 关闭消费者
        ConsumerManager consumerManager = this.brokerController.getConsumerManager();
        consumerManager.doChannelCloseEvent(remoteAddr, channel);

        // 关闭过滤器
        this.brokerController.getFilterServerManager().doChannelCloseEvent(remoteAddr, channel);
    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        // 关闭生产者
        ProducerManager producerManager = this.brokerController.getProducerManager();
        producerManager.doChannelCloseEvent(remoteAddr, channel);

        // 关闭消费者
        ConsumerManager consumerManager = this.brokerController.getConsumerManager();
        consumerManager.doChannelCloseEvent(remoteAddr, channel);

        // 关闭过滤器
        this.brokerController.getFilterServerManager().doChannelCloseEvent(remoteAddr, channel);
    }
}

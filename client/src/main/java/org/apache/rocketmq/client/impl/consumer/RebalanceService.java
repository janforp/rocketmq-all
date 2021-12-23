package org.apache.rocketmq.client.impl.consumer;

import lombok.AllArgsConstructor;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.ServiceThread;

@AllArgsConstructor
public class RebalanceService extends ServiceThread {

    private static final long waitInterval = Long.parseLong(System.getProperty("rocketmq.client.rebalance.waitInterval", "20000"));

    private final MQClientInstance mqClientFactory;

    @Override
    public void run() {
        while (!this.isStopped()) {
            // 只要服务没有停下来，就会一直循环

            // 等待一会,避免线程将 cpu 资源占死
            this.waitForRunning(waitInterval);

            // 执行任务
            this.mqClientFactory.doRebalance();
        }
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}
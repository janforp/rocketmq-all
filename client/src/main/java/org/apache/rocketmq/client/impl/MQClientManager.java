package org.apache.rocketmq.client.impl;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

// 客户端实例管理器
public class MQClientManager {

    private final static InternalLogger log = ClientLogger.getLog();

    private static final MQClientManager instance = new MQClientManager();

    // 计数用
    private final AtomicInteger factoryIndexGenerator = new AtomicInteger();

    /**
     * 控制每个JVM进程只有一个实例
     *
     * key:clientId
     * value:mq客户端实例
     */
    private final ConcurrentMap<String/*clientId：IP&instanceName（当前进程PID），如：10.201.13.28@9738*/, MQClientInstance> factoryTable = new ConcurrentHashMap<String, MQClientInstance>();

    private MQClientManager() {
        // 单例
    }

    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig) {
        return getOrCreateMQClientInstance(clientConfig, null);
    }

    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {

        // 构建客户端Id：IP&instanceName（当前进程PID），如：10.201.13.28@9738
        String clientId = clientConfig.buildMQClientId();
        MQClientInstance instance = this.factoryTable.get(clientId);
        if (null == instance) {
            // 首次拿不到，则创建
            ClientConfig cloneClientConfig = clientConfig.cloneClientConfig();
            int generatorAndIncrement = this.factoryIndexGenerator.getAndIncrement();
            instance = new MQClientInstance(cloneClientConfig, generatorAndIncrement, clientId/*10.201.13.28@9738*/, rpcHook);

            // 放进去缓存
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                // 防止并发创建，最终还是以第一个创建成功的实例为准
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }

        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}

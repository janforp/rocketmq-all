package org.apache.rocketmq.remoting.netty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NettyClientConfig {

    /**
     * Worker thread number
     */
    // 客户端工作线程
    private int clientWorkerThreads = 4;

    // 客户端回调处理线程池，线程数：平台核心数
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();

    // 单向请求并发度控制：65535
    private int clientOnewaySemaphoreValue = NettySystemConfig.CLIENT_ONEWAY_SEMAPHORE_VALUE;

    // 异步，65535
    private int clientAsyncSemaphoreValue = NettySystemConfig.CLIENT_ASYNC_SEMAPHORE_VALUE;

    // 客户端链接服务器超时时间限制
    private int connectTimeoutMillis = 3000;

    // 客户端未激活周期，60s 指定时间内 ch 未激活，需要关闭
    private long channelNotActiveInterval = 1000 * 60;

    /**
     * IdleStateEvent will be triggered when neither read nor write was performed for
     * the specified period of this time. Specify {@code 0} to disable
     *
     * ch 的最大空闲时间
     */
    private int clientChannelMaxIdleTimeSeconds = 120;

    // 底层 socket 写缓冲区大小
    private int clientSocketSndBufSize = NettySystemConfig.socketSndbufSize; // 65535

    // 底层 socket 收缓冲区大小
    private int clientSocketRcvBufSize = NettySystemConfig.socketRcvbufSize; // 65535

    // 客户端 netty 是否启用 内存池
    private boolean clientPooledByteBufAllocatorEnable = false;

    // 客户端在超时的时候是否主动关闭
    private boolean clientCloseSocketIfTimeout = false;

    private boolean useTLS;
}
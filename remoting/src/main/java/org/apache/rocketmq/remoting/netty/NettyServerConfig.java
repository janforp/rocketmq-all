package org.apache.rocketmq.remoting.netty;

import lombok.Getter;
import lombok.Setter;

public class NettyServerConfig implements Cloneable {

    /**
     * 服务端启动监听端口，会修改为 9876
     */
    @Getter
    @Setter
    private int listenPort = 8888;

    @Getter
    @Setter
    private int serverWorkerThreads = 8;

    @Getter
    @Setter
    private int serverCallbackExecutorThreads = 0;

    @Getter
    @Setter
    private int serverSelectorThreads = 3;

    /**
     * // 服务器向 客户端主动发起请求时的并发限制
     * //1.单向请求的并发限制
     */
    @Getter
    @Setter
    private int serverOnewaySemaphoreValue = 256;

    /**
     * // 服务器向 客户端主动发起请求时的并发限制
     * //2.异步请求的并发限制
     */
    @Getter
    @Setter
    private int serverAsyncSemaphoreValue = 64;

    /**
     * channel 最大的空闲存活时间
     */
    @Getter
    @Setter
    private int serverChannelMaxIdleTimeSeconds = 120;

    // 写缓冲区 65535
    @Getter
    @Setter
    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;

    // 读缓冲区 65535
    @Getter
    @Setter
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;

    // 是否开启 netty 内存池
    @Getter
    @Setter
    private boolean serverPooledByteBufAllocatorEnable = true;

    /**
     * make make install
     *
     *
     * ../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \
     * --host=x86_64-linux-gnu \ --build=x86_64-pc-linux-gnu \ --without-gd
     */
    @Getter
    @Setter
    private boolean useEpollNativeSelector = false;

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}

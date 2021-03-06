package org.apache.rocketmq.remoting.netty;

import lombok.Getter;
import lombok.Setter;

public class NettyServerConfig implements Cloneable {

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

    @Getter
    @Setter
    private int serverOnewaySemaphoreValue = 256;

    @Getter
    @Setter
    private int serverAsyncSemaphoreValue = 64;

    @Getter
    @Setter
    private int serverChannelMaxIdleTimeSeconds = 120;

    @Getter
    @Setter
    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;

    @Getter
    @Setter
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;

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
        return (NettyServerConfig) super.clone();
    }
}

package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;

/**
 * namesrv 跟 broker 会有，客户端(生产者或者发送者)是没有的！！
 */
public interface ChannelEventListener {

    void onChannelConnect(final String remoteAddr, final Channel channel);

    void onChannelClose(final String remoteAddr, final Channel channel);

    void onChannelException(final String remoteAddr, final Channel channel);

    void onChannelIdle(final String remoteAddr, final Channel channel);
}
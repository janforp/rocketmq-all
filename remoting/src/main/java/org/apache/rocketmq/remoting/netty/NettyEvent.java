package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class NettyEvent {

    private final NettyEventType type;

    private final String remoteAddr;

    private final Channel channel;

    @Override
    public String toString() {
        return "NettyEvent [type=" + type + ", remoteAddr=" + remoteAddr + ", channel=" + channel + "]";
    }
}

package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.ChannelEventListener;

@AllArgsConstructor
public class BrokerHousekeepingService implements ChannelEventListener {

    private final NamesrvController namesrvController;

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {
    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        channelDestroy(remoteAddr, channel);
    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        channelDestroy(remoteAddr, channel);
    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        channelDestroy(remoteAddr, channel);
    }

    private void channelDestroy(String remoteAddr, Channel channel) {
        RouteInfoManager routeInfoManager = this.namesrvController.getRouteInfoManager();
        routeInfoManager.onChannelDestroy(remoteAddr, channel);
    }
}
package org.apache.rocketmq.broker.longpolling;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageFilter;

@AllArgsConstructor
@Getter
public class PullRequest {

    // 网络请求对象
    private final RemotingCommand requestCommand;

    // 服务器跟客户端会话通道
    private final Channel clientChannel;

    // 长轮询超时时间
    private final long timeoutMillis;

    // 轮询开始时间
    private final long suspendTimestamp;

    // 查询开始 offset
    private final long pullFromThisOffset;

    // 订阅数据
    private final SubscriptionData subscriptionData;

    // 过滤器
    private final MessageFilter messageFilter;
}
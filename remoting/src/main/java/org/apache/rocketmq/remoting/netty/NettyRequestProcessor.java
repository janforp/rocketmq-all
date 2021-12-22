package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Common remoting command processor
 */
public interface NettyRequestProcessor {

    /**
     * 统一的请求方法，处理不同类型的请求 {@link  org.apache.rocketmq.common.protocol.RequestCode}
     *
     * @param ctx 上下文
     * @param request 请求
     * @return 结果
     * @throws Exception 异常
     */
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception;

    /**
     * 决定是否拒绝请求
     *
     * @return 是否拒绝
     */
    boolean rejectRequest();
}
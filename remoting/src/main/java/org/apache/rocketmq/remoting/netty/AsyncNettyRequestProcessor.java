package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public abstract class AsyncNettyRequestProcessor implements NettyRequestProcessor {

    // 其实是同步调用，除非子类覆盖了这个方法，自己去实现异步的逻辑
    public void asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request, RemotingResponseCallback responseCallback) throws Exception {
        //模版方法
        RemotingCommand response = processRequest(ctx, request);

        // 进入到响应客户端的逻辑
        responseCallback.callback(response);
    }
}
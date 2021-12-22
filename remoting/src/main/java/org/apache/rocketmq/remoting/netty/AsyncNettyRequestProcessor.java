package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public abstract class AsyncNettyRequestProcessor implements NettyRequestProcessor {

    /**
     * 其实是同步调用，除非子类覆盖了这个方法，自己去实现异步的逻辑
     *
     * @param ctx 上下文
     * @param request 请求
     * @param responseCallback 响应回调
     * @throws Exception 异常
     */
    public void asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request, RemotingResponseCallback responseCallback) throws Exception {
        /*
         * 模版方法
         * 调用子类实现的具体处理请求的方法拿到响应
         */
        RemotingCommand response = processRequest(ctx, request);

        /*
         * 拿到返回之后再调用回调
         * 进入到响应客户端的逻辑
         */
        responseCallback.callback(response);
    }
}
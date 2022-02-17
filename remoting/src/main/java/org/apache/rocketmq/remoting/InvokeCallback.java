package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.netty.ResponseFuture;

/**
 * 网络层回调
 *
 * @see RemotingClient#invokeAsync(java.lang.String, org.apache.rocketmq.remoting.protocol.RemotingCommand, long, org.apache.rocketmq.remoting.InvokeCallback)
 */
public interface InvokeCallback {

    void operationComplete(final ResponseFuture responseFuture);
}
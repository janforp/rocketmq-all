package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.netty.ResponseFuture;

public interface InvokeCallback {

    void operationComplete(final ResponseFuture responseFuture);
}

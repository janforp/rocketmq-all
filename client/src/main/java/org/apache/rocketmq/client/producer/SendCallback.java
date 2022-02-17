package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.remoting.InvokeCallback;

/**
 * 异步发送回调，注意与网络层回调{@link InvokeCallback}区别开来
 */
public interface SendCallback {

    void onSuccess(final SendResult sendResult);

    void onException(final Throwable e);
}
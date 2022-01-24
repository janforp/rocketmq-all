package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.impl.MQClientAPIImpl;

/**
 * Async message pulling interface
 */
public interface PullCallback {

    /**
     * @see MQClientAPIImpl#processPullResponse(org.apache.rocketmq.remoting.protocol.RemotingCommand)
     */
    void onSuccess(final PullResult pullResult);

    void onException(final Throwable e);
}
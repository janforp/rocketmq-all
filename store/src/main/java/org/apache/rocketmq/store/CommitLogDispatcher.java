package org.apache.rocketmq.store;

/**
 * Dispatcher of commit log.
 */
public interface CommitLogDispatcher {

    void dispatch(final DispatchRequest request);
}
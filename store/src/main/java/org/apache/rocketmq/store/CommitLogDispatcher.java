package org.apache.rocketmq.store;

/**
 * Dispatcher of commit log.
 */
public interface CommitLogDispatcher {

    /**
     * 主要在 commitLog 存储之后再创建 consumeQueue 以及 IndexFile
     *
     * @param request 请求
     */
    void dispatch(final DispatchRequest request);
}
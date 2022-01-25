package org.apache.rocketmq.store;

/**
 * @see DefaultMessageStore#getMessage(java.lang.String, java.lang.String, int, long, int, org.apache.rocketmq.store.MessageFilter)
 */
public enum GetMessageStatus {

    /**
     * 查询到消息
     */
    FOUND,

    /**
     * 未查询到消息：服务端消息过滤tagCode
     */
    NO_MATCHED_MESSAGE,

    /**
     * 查询时，正好赶上 commitLog 清理过期文件，导致查询失败
     */
    MESSAGE_WAS_REMOVING,

    /**
     * 查询时，正好赶上 consumeQueue 清理过期文件，导致查询失败
     */
    OFFSET_FOUND_NULL,

    /**
     * pullRequest.offset 越界 maxOffset
     */
    OFFSET_OVERFLOW_BADLY,

    /**
     * 最常见的情况！！！！
     *
     * 客户端的消费进度跟当前消息队列持平了，需要长轮询
     *
     * pullRequest.offset == CQ.maxOffset
     */
    OFFSET_OVERFLOW_ONE,

    /**
     * pullRequest.offset 越界 minOffset
     */
    OFFSET_TOO_SMALL,

    /**
     * 不存在这种情况
     */
    NO_MATCHED_LOGIC_QUEUE,

    /**
     * 空队列，创建队列也是因为查询导致的
     */
    NO_MESSAGE_IN_QUEUE,
}
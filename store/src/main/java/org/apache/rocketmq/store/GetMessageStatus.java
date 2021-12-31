package org.apache.rocketmq.store;

public enum GetMessageStatus {

    // 查询到消息；
    FOUND,

    NO_MATCHED_MESSAGE,

    MESSAGE_WAS_REMOVING,

    //
    OFFSET_FOUND_NULL,

    OFFSET_OVERFLOW_BADLY,

    // 客户端的消费进度跟当前消息队列持平了，需要长轮询
    OFFSET_OVERFLOW_ONE,

    OFFSET_TOO_SMALL,

    NO_MATCHED_LOGIC_QUEUE,

    NO_MESSAGE_IN_QUEUE,
}
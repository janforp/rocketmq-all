package org.apache.rocketmq.client.producer;

public enum SendStatus {
    SEND_OK,

    // 刷盘超时
    FLUSH_DISK_TIMEOUT,

    //
    FLUSH_SLAVE_TIMEOUT,

    // diaoxian
    SLAVE_NOT_AVAILABLE,
}

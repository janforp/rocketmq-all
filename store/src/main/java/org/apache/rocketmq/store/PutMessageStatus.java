package org.apache.rocketmq.store;

public enum PutMessageStatus {

    PUT_OK,

    FLUSH_DISK_TIMEOUT,

    FLUSH_SLAVE_TIMEOUT,

    SLAVE_NOT_AVAILABLE,

    SERVICE_NOT_AVAILABLE,

    CREATE_MAPEDFILE_FAILED,

    MESSAGE_ILLEGAL,

    PROPERTIES_SIZE_EXCEEDED,

    /**
     * 操作系统页缓存繁忙
     */
    OS_PAGECACHE_BUSY,

    UNKNOWN_ERROR,
}
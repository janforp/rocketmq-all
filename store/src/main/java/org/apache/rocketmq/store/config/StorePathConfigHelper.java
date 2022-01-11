package org.apache.rocketmq.store.config;

import java.io.File;

public class StorePathConfigHelper {

    public static String getStorePathConsumeQueue(final String rootDir /* /users/zhuchenjian/store */) {
        // /users/zhuchenjian/store/consumequeue
        return rootDir + File.separator + "consumequeue";
    }

    public static String getStorePathConsumeQueueExt(final String rootDir) {
        // /users/zhuchenjian/store/consumequeue_ext
        return rootDir + File.separator + "consumequeue_ext";
    }

    public static String getStorePathIndex(final String rootDir) {
        return rootDir + File.separator + "index";
    }

    public static String getStoreCheckpoint(final String rootDir) {
        return rootDir + File.separator + "checkpoint";
    }

    /**
     * 该文件 /users/zhuchenjian/store/abort 是否存在，决定上次是否是正常退出
     */
    public static String getAbortFile(final String rootDir/* /users/zhuchenjian/store */) {
        // /users/zhuchenjian/store/abort
        return rootDir + File.separator + "abort";
    }

    public static String getLockFile(final String rootDir) {
        return rootDir + File.separator + "lock";
    }

    public static String getDelayOffsetStorePath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "delayOffset.json";
    }

    public static String getTranStateTableStorePath(final String rootDir) {
        return rootDir + File.separator + "transaction" + File.separator + "statetable";
    }

    public static String getTranRedoLogStorePath(final String rootDir) {
        return rootDir + File.separator + "transaction" + File.separator + "redolog";
    }

}

package org.apache.rocketmq.store;

import java.util.Map;

public interface MessageArrivingListener {

    /**
     * 当有消息过来当时候会调用该接口
     */
    void arriving(String topic, int queueId, long logicOffset/*当前队列的最大的 offset*/, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties);
}

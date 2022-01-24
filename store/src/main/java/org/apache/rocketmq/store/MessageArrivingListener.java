package org.apache.rocketmq.store;

import java.util.Map;

public interface MessageArrivingListener {

    void arriving(String topic, int queueId, long logicOffset/*当前队列的最大的 offset*/, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties);
}

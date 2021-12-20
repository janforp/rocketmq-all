package org.apache.rocketmq.store;

import java.util.Map;

public interface MessageArrivingListener {

    void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties);
}

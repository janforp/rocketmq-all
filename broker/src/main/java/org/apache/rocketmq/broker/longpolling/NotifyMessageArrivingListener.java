package org.apache.rocketmq.broker.longpolling;

import lombok.AllArgsConstructor;
import org.apache.rocketmq.store.MessageArrivingListener;

import java.util.Map;

@AllArgsConstructor
public class NotifyMessageArrivingListener implements MessageArrivingListener {

    private final PullRequestHoldService pullRequestHoldService;

    @Override
    public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        this.pullRequestHoldService.notifyMessageArriving(topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties);
    }
}
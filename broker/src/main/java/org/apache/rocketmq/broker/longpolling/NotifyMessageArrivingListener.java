package org.apache.rocketmq.broker.longpolling;

import lombok.AllArgsConstructor;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;

import java.util.Map;

@AllArgsConstructor
public class NotifyMessageArrivingListener implements MessageArrivingListener {

    private final PullRequestHoldService pullRequestHoldService;

    /**
     * @see DefaultMessageStore.ReputMessageService#doReput()
     */
    @Override
    public void arriving(String topic, int queueId, long logicOffset/*当前队列的最大的 offset*/, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        this.pullRequestHoldService.notifyMessageArriving(topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties);
    }
}
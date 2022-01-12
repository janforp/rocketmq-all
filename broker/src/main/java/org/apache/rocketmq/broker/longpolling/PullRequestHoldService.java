package org.apache.rocketmq.broker.longpolling;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 拉消息挂起逻辑
 */
public class PullRequestHoldService extends ServiceThread {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final String TOPIC_QUEUEID_SEPARATOR = "@";

    private final BrokerController brokerController;

    private final SystemClock systemClock = new SystemClock();

    private final ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable = new ConcurrentHashMap<>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        // 加入到该 主题 归属的 manyPullReqyest 中
        mpr.addPullRequest(pullRequest);
    }

    private String buildKey(final String topic, final int queueId) {
        return topic + TOPIC_QUEUEID_SEPARATOR + queueId;
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    // 服务器开启来长轮询，等待五秒
                    this.waitForRunning(5 * 1000);
                } else {
                    // 等待一秒
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    private void checkHoldRequest() {

        // ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable
        for (String key : this.pullRequestTable.keySet()) {

            //topic@queueId

            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0]; // 主题
                int queueId = Integer.parseInt(kArray[1]); // 队列号
                // 查询当前队列最大的 offset
                MessageStore messageStore = this.brokerController.getMessageStore();
                final long offset = messageStore.getMaxOffsetInQueue(topic, queueId);
                try {
                    // 通知消息到达的逻辑
                    this.notifyMessageArriving(topic, queueId, offset /*当前队列最大的 offset */);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset/*当前队列最大的 offset */) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    /**
     * @see NotifyMessageArrivingListener#arriving(java.lang.String, int, long, long, long, byte[], java.util.Map)
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {

        //topic@queueId
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr == null) {
            return;
        }
        List<PullRequest> requestList = mpr.cloneListAndClear();
        if (requestList == null) {
            return;
        }

        // 重放列表，当某个 pullRequest 既不超时，也没有超时的话，就再次放入
        List<PullRequest> replayList = new ArrayList<>();

        for (PullRequest request : requestList) {
            long newestOffset = maxOffset;
            if (newestOffset <= request.getPullFromThisOffset()) {
                // 说明在循环节点已经有新数据加入。保证 newestOffset 为队列的 maxOffset
                MessageStore messageStore = this.brokerController.getMessageStore();
                newestOffset = messageStore.getMaxOffsetInQueue(topic, queueId);
            }

            if (newestOffset > request.getPullFromThisOffset()) {
                // 说明有消息来，长轮询可以结束了

                MessageFilter messageFilter = request.getMessageFilter();
                boolean match = messageFilter.isMatchedByConsumeQueue(tagsCode, new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                // match by bit map, need eval again when properties is not null.
                if (match && properties != null) {
                    match = messageFilter.isMatchedByCommitLog(null, properties);
                }

                if (match) {
                    try {

                        // 匹配，则再次发起拉消息
                        // 将满足条件的 pullRequest 再次封装成 RequestTask 提交到线程池，发起拉消息的请求
                        PullMessageProcessor pullMessageProcessor = this.brokerController.getPullMessageProcessor();
                        pullMessageProcessor.executeRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                    } catch (Throwable e) {
                        log.error("execute request when wakeup failed.", e);
                    }
                    continue;
                }
            }

            // 上面没有匹配，则执行下面代码
            if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                // 超时的
                try {

                    // 如果超时，则再次提交到请求
                    PullMessageProcessor pullMessageProcessor = this.brokerController.getPullMessageProcessor();
                    pullMessageProcessor.executeRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                } catch (Throwable e) {
                    log.error("execute request when wakeup failed.", e);
                }
                continue;
            }

            // 其他的继续添加到list，下次继续
            replayList.add(request);
        }

        if (!replayList.isEmpty()) {
            // 把剩下的再放入list
            mpr.addPullRequest(replayList);
        }
    }
}

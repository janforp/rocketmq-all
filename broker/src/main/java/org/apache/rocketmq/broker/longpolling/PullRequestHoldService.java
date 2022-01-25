package org.apache.rocketmq.broker.longpolling;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.common.BrokerConfig;
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
 *
 * 1.客户端到服务端拉消息，暂时没有消息的时候，需要进行轮询
 * 2.
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
        String key/*topic@queueId*/ = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key/*topic@queueId*/);
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
                BrokerConfig brokerConfig = this.brokerController.getBrokerConfig();
                if (brokerConfig.isLongPollingEnable()) {
                    // 服务器开启来长轮询，等待五秒
                    this.waitForRunning(5 * 1000);
                } else {
                    // 等待一秒
                    this.waitForRunning(brokerConfig.getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();

                // 核心业务逻辑
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

    private void checkHoldRequest() {
        MessageStore messageStore = this.brokerController.getMessageStore();

        // ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable
        for (String key/*topic@queueId*/ : this.pullRequestTable.keySet()) {

            //topic@queueId

            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0]; // 主题
                int queueId = Integer.parseInt(kArray[1]); // 队列号
                // 查询当前队列最大的 offset

                // 当前队列的最大的 逻辑 offset （从1开始，没存储一条消息 +1）
                final long offset = messageStore.getMaxOffsetInQueue(topic, queueId);
                try {
                    // 通知消息到达的逻辑
                    this.notifyMessageArriving(topic, queueId, offset /*当前队列最大的 逻辑 offset （从1开始，没存储一条消息 +1） */);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset/*当前队列最大的 逻辑 offset （从1开始，没存储一条消息 +1） */) {
        notifyMessageArriving(topic, queueId, maxOffset/*当前队列最大的 逻辑 offset （从1开始，没存储一条消息 +1） */, null, 0, null, null);
    }

    /**
     * 有消息来的时候发出通知
     *
     * @see NotifyMessageArrivingListener#arriving(java.lang.String, int, long, long, long, byte[], java.util.Map) 调用该方法的第二种情况就是这里
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset/*当前队列最大的 逻辑 offset （从1开始，没存储一条消息 +1） */, final Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {

        String key/*topic@queueId*/ = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr == null) {
            return;
        }

        // 拿到全部的队列列表
        List<PullRequest> requestList = mpr.cloneListAndClear();
        if (requestList == null) {
            return;
        }

        // 重放列表，当某个 pullRequest 既不超时，也没有超时的话，就再次放入
        List<PullRequest> replayList/*继续长轮询的请求*/ = new ArrayList<>();

        MessageStore messageStore = this.brokerController.getMessageStore();

        for (PullRequest request : requestList) {
            long newestOffset = maxOffset;
            if (newestOffset <= request.getPullFromThisOffset()/*查询开始 offset*/) {
                // 说明在循环节点已经有新数据加入。保证 newestOffset 为队列的 maxOffset
                newestOffset = messageStore.getMaxOffsetInQueue(topic, queueId);
            }

            if (newestOffset > request.getPullFromThisOffset() /* 当前队列的最大offset 大于 拉消息请求的  offset，说明该队列在长轮询期间有消息存入了，可以去尝试拉消息 */) {
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

                    // 继续循环，不会往下面执行了
                    continue;
                }
            }

            // 上面没有匹配，则执行下面代码
            if (System.currentTimeMillis() >= (request.getSuspendTimestamp()/*长轮询开始时间*/ + request.getTimeoutMillis()) /*长轮询超时了*/) {
                // 超时的，再发起一次拉消息请求，但是不会再次进入长轮询逻辑
                try {

                    // 如果超时，则再次提交到请求
                    PullMessageProcessor pullMessageProcessor = this.brokerController.getPullMessageProcessor();
                    pullMessageProcessor.executeRequestWhenWakeup(request.getClientChannel(), request.getRequestCommand());
                } catch (Throwable e) {
                    log.error("execute request when wakeup failed.", e);
                }

                // 继续循环，不会往下面执行了
                continue;
            }

            // 该请求关心的队列中，既没有新消息，长轮询也没有超时，其他的继续添加到list，继续进行长轮询
            replayList.add(request);
        }

        if (!replayList.isEmpty()) {
            // 把剩下的再放入list
            mpr.addPullRequest(replayList);
        }
    }
}

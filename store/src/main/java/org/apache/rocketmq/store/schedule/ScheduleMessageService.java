package org.apache.rocketmq.store.schedule;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ScheduleMessageService extends ConfigManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";

    private static final long FIRST_DELAY_TIME = 1000L;

    private static final long DELAY_FOR_A_WHILE = 100L;

    private static final long DELAY_FOR_A_PERIOD = 10000L;

    /**
     * 延迟级别 - 延迟时间，默认支持18个延迟级别，时间分别从1s到2h
     * load 方法初始化，通过配置加载
     */
    private final ConcurrentMap<Integer /* level 延迟级别 */, Long/* delay timeMillis 具体延迟时间 */> delayLevelTable = new ConcurrentHashMap<>(32);

    // 每个延迟级别都有自己的 queue ，这个map 就记录来每个延迟级别对于队列的消费进度
    private final ConcurrentMap<Integer /* level 延迟级别 */, Long/* offset 队列消费进度，每10秒持久化到磁盘一次 */> offsetTable = new ConcurrentHashMap<>(32);

    private final DefaultMessageStore defaultMessageStore;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private Timer timer;

    @Setter
    private MessageStore writeMessageStore;

    // 最大延迟级别
    @Getter
    private int maxDelayLevel;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        for (Map.Entry<Integer, Long> next : this.offsetTable.entrySet()) {
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    public void start() {
        if (started.compareAndSet(false, true)/*控制对象在关闭之前最多启动一次*/) {

            // 创建定时器对象，里面有自己的线程资源
            this.timer = new Timer("ScheduleMessageTimerThread", true);

            // 遍历所有的延迟级别，为每个延迟级别创建一个任务，提交到定时器中
            for (Map.Entry<Integer /* level */, Long /* delay timeMillis */> entry : this.delayLevelTable.entrySet()) {
                // 延迟级别
                Integer level = entry.getKey();
                // 延迟时间
                Long timeDelay = entry.getValue();
                // ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }

                if (timeDelay != null) {
                    // 延迟 1000L 之后执行 该任务
                    // 为每个延迟级别创建一个延迟任务
                    this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME/*100ms*/);
                }
            }

            // 提交一个：按间隔时间重复执行的任务
            // 延迟10s之后执行，以后每隔10s执行一次
            this.timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        if (started.get()) {
                            // 持久化 offsetTable 的值到 文件中
                            // 持久化延迟队列消费进度的定时任务
                            ScheduleMessageService.this.persist();// 每10秒持久化一次延迟队列的消费进度
                        }
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval()/*10秒*/);
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.timer) {
                this.timer.cancel();
            }
        }

    }

    public boolean isStarted() {
        return started.get();
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public boolean load() {
        // 从指定文件中加载配置，并且设置到内存中去
        // delayOffset.json
        // @see org.apache.rocketmq.store.schedule.ScheduleMessageService.decode
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        return result;
    }

    @Override
    public String configFilePath() {
        // rootDir + File.separator + "config" + File.separator + "delayOffset.json";
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    public boolean parseDelayLevel() {

        // 存储 s,分，时，天 对应的 ms 数值
        HashMap<String, Long> timeUnitTable = new HashMap<>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        // String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
        // 默认支持的延迟级别 1s 到 2h
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            // 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h 按空格拆分
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {

                // 如1s，2m，1h等
                String value = levelArray[i];
                // 拿到时间单位，如：s,m,h（秒分时）
                String ch = value.substring(value.length() - 1);
                // 拿到单位对应的ms值
                Long tu/*time unit*/ = timeUnitTable.get(ch);

                // 延迟级别从1开始
                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    // 不能超过最大的延迟级别，保存最大的延迟级别
                    this.maxDelayLevel = level;
                }

                // 获取数值
                long num = Long.parseLong(value.substring(0, value.length() - 1));

                // 数值 乘以 时间单位 就是延迟的总时间
                long delayTimeMillis = tu * num;
                // 存储到内存中，方便后面使用
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    @AllArgsConstructor
    class DeliverDelayedMessageTimerTask extends TimerTask {

        // 延迟级别
        private final int delayLevel;

        // 延迟队列的消费进度
        private final long offset;

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    this.executeOnTimeUp();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {
            long result = deliverTimestamp;
            long maxTimestamp = now + ScheduleMessageService.this/*非静态内部类可以使用的语法*/.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                // 说明交付时间有问题，调整下
                result = now;
            }

            return result;
        }

        public void executeOnTimeUp() {

            // 拿到消费队列
            int queueId = delayLevel2QueueId(delayLevel);
            ConsumeQueue cq = ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(SCHEDULE_TOPIC, queueId);
            long failScheduleOffset = offset;

            if (cq != null) {

                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        long nextOffset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE /*每次读取20个字节*/) {
                            long offsetPy = bufferCQ.getByteBuffer().getLong(); // 物理偏移量
                            int sizePy = bufferCQ.getByteBuffer().getInt(); // 大小
                            /**
                             * @see ScheduleMessageService#computeDeliverTimestamp(int, long)
                             * 延迟消息的交付时间！！！！！！ 跟正常消息不一样
                             */
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}", tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }

                            long now = System.currentTimeMillis();

                            // 校验下交付时间是正确的，并且返回交付时间
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            // 计算下一条消息的 offset（CQData 的）
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE /*常量为20*/);

                            long countdown = deliverTimestamp - now;

                            if (countdown <= 0) {// 到达交付时间了

                                // 查询消息
                                MessageExt msgExt = ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy) /* 从 commitLog 中查询消息 */;

                                if (msgExt != null) {
                                    try {

                                        // 创建新消息,并且设置消息的主题跟队列为原始消息的主题跟队列，清除了延迟消息的属性
                                        MessageExtBrokerInner msgInner = this.messageTimeUp(msgExt);
                                        if (MixAll.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                                            // 半消息不投递
                                            log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}", msgInner.getTopic(), msgInner);
                                            continue;
                                        }
                                        // 将新消息存储到 commitLog
                                        // 最终 ReputMessageService 会向 目标主题的 ConsumeQueue 中添加 CQData
                                        // 作为消费者订阅的是目标主题，所以会再次消费该消息
                                        PutMessageResult putMessageResult = ScheduleMessageService.this.writeMessageStore.putMessage(msgInner);

                                        if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            // empty
                                        } else {
                                            // XXX: warn and notify me
                                            log.error("ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}", msgExt.getTopic(), msgExt.getMsgId());
                                            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset), DELAY_FOR_A_PERIOD/*10000 ms*/);
                                            ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me
                                         */
                                        log.error("ScheduleMessageService, messageTimeup execute error, drop it. msgExt=" + msgExt + ", nextOffset=" + nextOffset + ",offsetPy=" + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }
                            } else {
                                // 还没到延迟级别对应的延迟时间呢
                                // 重新提交该延迟级别的任务
                                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset), countdown);

                                // 更新下该延迟级别队列的进度
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset), DELAY_FOR_A_WHILE/*100ms*/);
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {
                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {
                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset=" + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            // 重新提交该延迟级别的延迟任务
            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel, failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        private MessageExtBrokerInner messageTimeUp(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());

            // 注意
            long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);

            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);

            // 清理新消息的 DELAY 属性，为啥呢？如果不清除，回头存储的时候会又被转发到延迟队列
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            // 目标主题
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            // 目标队列
            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}

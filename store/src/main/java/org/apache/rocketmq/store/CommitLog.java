package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.ha.WaitNotifyObject;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 提交日志，顺序写入
 * Store all metadata downtime for recovery, data protection reliability
 *
 * @see CommitLog#load() 1
 * @see CommitLog#recoverNormally(long) 2
 * @see CommitLog#start() 3
 */
@SuppressWarnings("all")
public class CommitLog {

    /**
     * 正常消息的魔法值
     * 存储到 CommitLog 文件时，消息的第一个字段是 msgSize 第二个字段就是 该魔法值
     *
     * 二进制：11011010101000110010000010100111
     * 十六进制：daa320a7
     */
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;

    public static void main(String[] args) {
        System.out.println(Integer.toBinaryString(MESSAGE_MAGIC_CODE).length());
        System.out.println(Integer.toHexString(MESSAGE_MAGIC_CODE));
        System.out.println(Integer.toBinaryString(BLANK_MAGIC_CODE));
    }

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * End of file empty MAGIC CODE cbd43194
     * 标记一个文件结尾的魔法值！
     *
     * 文件尾消息魔法值
     * 11001011110101000011000110010100
     */
    protected final static int BLANK_MAGIC_CODE = -875286124;

    // 目录，管理目录下的文件列表
    protected final MappedFileQueue mappedFileQueue;

    // 存储的主模块
    protected final DefaultMessageStore defaultMessageStore;

    /**
     * 具体在构造方法中初始化
     *
     * @see GroupCommitService 同步刷盘
     * @see FlushRealTimeService 默认是异步刷盘
     * @see CommitLog#start() 该对象的 start 方法会启动刷盘服务
     */
    // 刷盘服务：里面有自己的线程，实现异步刷盘
    private final FlushCommitLogService flushCommitLogService;

    // If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
    // 如果启用了 TransientStorePool，我们必须定期将消息刷新到 FileChannel

    /**
     * this.commitLogService = new CommitRealTimeService();
     *
     * @see CommitRealTimeService
     */
    private final FlushCommitLogService commitLogService;

    /**
     * 控制消息中哪些字段追加到 mappedFile
     *
     * 追加消息到文件的具体逻辑由该对象控制！！！
     *
     * TODO 这是一个有状态的对象，但是是 commitLog 共享的？为啥呢？因为写入消息的方法是串行的，根本就没有并发问题，所以不需要考虑线程安全问题
     *
     * @see DefaultAppendMessageCallback
     */
    private final AppendMessageCallback appendMessageCallback;

    private final ThreadLocal<MessageExtBatchEncoder> batchEncoderThreadLocal;

    /**
     * 主题下各消费队列的偏移量，队列偏移量字典表
     * key：主题下的队列id
     * value:偏移量 (key:队列id，value:该队列的偏移量)
     *
     * @see DefaultAppendMessageCallback#doAppend(long, java.nio.ByteBuffer, int, org.apache.rocketmq.store.MessageExtBrokerInner)
     *
     * CommitLog.this.topicQueueTable.put(key, ++queueOffset) 队列添加了一条消息，则逻辑偏移量++------逻辑偏移量
     */
    @Setter
    @Getter
    protected HashMap<String/** topic-queueid */, Long/** 该队列的逻辑offset ，逻辑偏移量*/> topicQueueTable = new HashMap<String, Long>(1024);

    /**
     * TODO ????
     */
    @Getter
    @Setter
    protected volatile long confirmOffset = -1L;

    // 写锁开始时间

    /**
     * commitLog 写入是串行的，所以需要加锁！！！
     *
     * 开始写到时候赋值，写完成的时候恢复为0
     *
     * @see CommitLog#putMessage(org.apache.rocketmq.store.MessageExtBrokerInner)
     */
    @Getter
    private volatile long beginTimeInLock = 0;

    /**
     * @see PutMessageReentrantLock 重入锁，默认
     * @see PutMessageSpinLock 自旋锁，线程不会挂起
     * @see MessageStoreConfig#useReentrantLockWhenPutMessage 默认是自旋锁
     */
    // 锁的实现分为：自旋锁(消耗cpu)跟重入锁
    protected final PutMessageLock putMessageLock;

    public CommitLog(final DefaultMessageStore defaultMessageStore) {
        MessageStoreConfig messageStoreConfig = defaultMessageStore.getMessageStoreConfig();

        String storePathCommitLog = messageStoreConfig.getStorePathCommitLog();/*commitlog目录*/
        int mappedFileSizeCommitLog = messageStoreConfig.getMappedFileSizeCommitLog();
        AllocateMappedFileService allocateMappedFileService = defaultMessageStore.getAllocateMappedFileService();
        this.mappedFileQueue = new MappedFileQueue(storePathCommitLog,/*commitlog目录*/mappedFileSizeCommitLog,/*1G*/allocateMappedFileService/*里面有自己的线程，创建文件的时候可以通过该对象实现*/);

        this.defaultMessageStore = defaultMessageStore;

        if (FlushDiskType.SYNC_FLUSH == messageStoreConfig.getFlushDiskType()) {
            this.flushCommitLogService = new GroupCommitService();
        } else {
            //默认异步刷盘
            this.flushCommitLogService = new FlushRealTimeService();
        }
        this.commitLogService = new CommitRealTimeService();

        // 1024 * 1024 * 4; 4M
        int maxMessageSize = messageStoreConfig.getMaxMessageSize();
        this.appendMessageCallback = new DefaultAppendMessageCallback(maxMessageSize);

        batchEncoderThreadLocal = ThreadLocal.withInitial(() -> new MessageExtBatchEncoder(maxMessageSize));

        /**
         * 创建写锁
         * @see MessageStoreConfig#useReentrantLockWhenPutMessage 默认是自旋锁
         */
        this.putMessageLock = messageStoreConfig.isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

    }

    /**
     * load -> recover -> start
     */

    public boolean load() {
        // 加载目录下的文件，创建mp
        // this.mappedFileQueue = new MappedFileQueue(storePathCommitLog,/*commitlog目录*/mappedFileSizeCommitLog,/*1G*/allocateMappedFileService/*里面有自己的线程，创建文件的时候可以通过该对象实现*/);
        // 具体加载哪个目录由该对象的构造方法决定
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        // 该服务内部有自己的线程
        this.flushCommitLogService/*启动刷盘/落盘服务*/.start();
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.start();
        }
    }

    public void shutdown() {
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.shutdown();
        }

        this.flushCommitLogService.shutdown();
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(final long expiredTime, final int deleteFilesInterval, final long intervalForcibly, final boolean cleanImmediately) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * Read CommitLog data, use data replication
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        // commitLog 文件的大小 1G
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        // 找到 包含 offset 的文件
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            // 其实就是得到了 pos 到 wrotePos 这段字节数组到内容
            return mappedFile.selectMappedBuffer(pos);
        }
        return null;
    }

    /**
     * 上次关机属于正常关机的时候，再次执行服务的时候会执行该方法进行数据恢复
     *
     * When the normal exit, data recovery, all memory data have been flush
     * mfq[mf1,mf2,mf3,mf4,mf5]
     * 恢复是从mf3开始的
     *
     * @param maxPhyOffsetOfConsumeQueue 存储主模块启动阶段，先恢复的是所有的 ConsumeQueue数据，再恢复 CommitLog 数据 ，maxPhyOffsetOfConsumeQueue 表示恢复阶段 ConsumeQueue 中已知的最大消息 offset
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue/*ConsumeQueue 中已知的最大消息 物理偏移量offset*/) {
        boolean checkCRCOnRecover/*是否检查crc,默认 true*/ = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles/*拿到每一个commitLog文件！*/ = this.mappedFileQueue.getMappedFiles();
        if (mappedFiles.isEmpty()) {
            // 目录下面没有 mappedFile 文件,则需要删除相应的文件
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            // commitLog 目录下面没有任何文件，则需要把之前创建的 consumeQueue 文件都销毁
            this.defaultMessageStore.destroyLogics();
            return;
        }
        // 目录下有 commitLog 文件
        // 进入恢复逻辑
        int index /*从倒数第三个*/ = mappedFiles.size() - 3;
        if (index < 0) {
            // 没有3个文件的时候从第一个文件开始
            index = 0;
        }
        MappedFile mappedFile/*拿到待恢复的commitLog文件*/ = mappedFiles.get(index);
        // MappedByteBuffer mappedByteBuffer; 该 commitLog 文件对应的内容缓存
        ByteBuffer byteBuffer/*复制，包含文件的所有数据*/ = mappedFile.sliceByteBuffer();
        long processOffset/*获取待恢复 mf 文件名作为 处理起始偏移量（目录恢复位点）*/ = mappedFile.getFileFromOffset();
        long mappedFileOffset/*待处理 mf 的处理位点（文件内的位点，从 0 开始,每处理完一个 commitLog 文件，在处理下一个文件的时候归零，如果没有下一个commitLog 文件，则停留在最后一个文件的写入位点）*/ = 0;
        // 循环所有的 commitLog 文件，恢复每条消息
        while (true) {
            DispatchRequest dispatchRequest /*特殊情况1.magic_code 表示文件尾，2.dispatchRequest.size == 0 表示文件内的消息都处理完；并且未达到文件尾*/ = this.checkMessageAndReturnSize/*从切片内解析一条 msg 封装成 一个 dispatchRequest 对象*/(byteBuffer, checkCRCOnRecover);
            int size = dispatchRequest.getMsgSize();
            // Normal data
            if (dispatchRequest.isSuccess() && size > 0) { // 正常情况走这里
                mappedFileOffset/*推进当前文件的偏移量*/ = mappedFileOffset + size;
            }
            // Come the end of the file, switch to the next file Since the return 0 representatives met last hole,this can not be included in truncate offset
            // 到文件结尾，切换到下一个文件 由于返回0代表遇到了最后一个洞，这个不能包含在truncate offset中
            else if (dispatchRequest.isSuccess() && size == 0) { // 文件尾的情况走这里
                // 该文件末尾了，当前文件没有数据可以刷盘了
                index++/*继续到下一个文件*/;
                if (index >= mappedFiles.size()/*没有下一个文件了，说明该目录下的文件都刷盘了，跳出循环*/) {
                    // Current branch can not happen
                    log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                    break;/* while 循环中断 */
                } else {
                    // 还没有到该目录下的最后一个文件，继续恢复下一个 commitLog 文件
                    mappedFile/*拿到待恢复的commitLog文件*/ = mappedFiles.get(index);
                    byteBuffer/*复制，包含文件的所有数据*/ = mappedFile.sliceByteBuffer();
                    processOffset/*获取待恢复 mf 文件名作为 处理起始偏移量（目录恢复位点）*/ = mappedFile.getFileFromOffset();
                    mappedFileOffset/*待处理 mf 的处理位点（文件内的位点，从 0 开始,每处理完一个 commitLog 文件，在处理下一个文件的时候归零，如果没有下一个commitLog 文件，则停留在最后一个文件的写入位点）*/ = 0;
                    log.info("recover next physics file, " + mappedFile.getFileName());
                    /* while 循环 继续 */
                }
            }
            // Intermediate file read error -- 中间文件读取错误
            else if (!dispatchRequest.isSuccess()) {
                // 该文件恢复数据失败了，则直接跳出循环
                log.info("recover physics file end, " + mappedFile.getFileName());
                break;/* while 循环中断 */
            }
        }
        // 跳出了 while 循环

        // 执行到这里。正常情况下，所有待恢复的数据 已经被检查一遍了
        // 再加 mappedFileOffset 之前，processOffset 是最后一个文件的文件名，再加上 mappedFileOffset，则表示 commitLog 的全局位点
        // long processOffset = mappedFile.getFileFromOffset();
        processOffset/*计算得到，commitLog全局的最大物理偏移量*/ = processOffset/*最后一个恢复文件的文件名，也就是起始偏移量*/ + mappedFileOffset/*待处理 mf 的处理位点（文件内的位点，从 0 开始,每处理完一个 commitLog 文件，在处理下一个文件的时候归零，如果没有下一个commitLog 文件，则停留在最后一个文件的写入位点）*/;
        this.mappedFileQueue.setFlushedWhere(processOffset/*commitLog全局的最大物理偏移量*/);
        this.mappedFileQueue.setCommittedWhere(processOffset/*commitLog全局的最大物理偏移量*/);
        // 调整 mfq 内当前正在顺序写的mf 的刷盘点和写入点
        this.mappedFileQueue.truncateDirtyFiles(processOffset);
        // Clear ConsumeQueue redundant data
        if (maxPhyOffsetOfConsumeQueue/*ConsumeQueue 中已知的最大消息 物理偏移量offset*/ >= processOffset/*commitLog全局的最大物理偏移量*/) /*commitLog全局的最大物理偏移量 都没有 ConsumeQueue 中已知的最大消息 物理偏移量offset 大，则说明 consumeQueue 中有垃圾数据*/ {
            log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
            this.defaultMessageStore.truncateDirtyLogicFiles/*删除 ConsumeQueue 中的脏文件*/(processOffset);
        }
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer/*MappedByteBuffer mappedByteBuffer; 该 commitLog 文件对应的内容缓存*/, final boolean checkCRC/*是否检查crc,默认 true*/) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * TODO 校验 mappedFile 中的下一条数据，检查它是否是一条完整并且正确的消息，或者是否已经达到了文件结尾，如果既不是一条消息也不是文件结尾，则说明该文件存储的时候发生了异常
     *
     *
     * 从 commitLog 对应的 byteBuffer 中读取一条消息
     *
     * check the message and returns the message size
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer/*MappedByteBuffer mappedByteBuffer; 该 commitLog 文件对应的内容缓存*/, final boolean checkCRC/*是否检查crc,默认 true*/, final boolean readBody) {
        try {
            /**
             *    4 //TOTALSIZE
             *  + 4 //MAGICCODE
             *  + 4 //BODYCRC
             *  + 4 //QUEUEID
             *  + 4 //FLAG
             *  + 8 //QUEUEOFFSET
             *  + 8 //PHYSICALOFFSET
             *  + 4 //SYSFLAG
             *  + 8 //BORNTIMESTAMP
             *  + bornhostLength //BORNHOST
             *  + 8 //STORETIMESTAMP
             *  + storehostAddressLength //STOREHOSTADDRESS
             *  + 4 //RECONSUMETIMES
             *  + 8 //Prepared Transaction Offset
             *  + 4 + (bodyLength > 0 ? bodyLength : 0)
             *   + //BODY
             *  + 1 + topicLength //TOPIC
             *  + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
             *  + 0;
             */

            // 1 TOTAL SIZE
            // 前四个字节
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE
            // 后四个字节，魔法值
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    // 魔法值是消息的，正常情况,当前可以拿到一条正常的消息
                    break;/* switch 中断，继续往下执行*/
                case BLANK_MAGIC_CODE:
                    // 魔法值是文件结尾标志
                    return new DispatchRequest(0/*文件结尾*/, true /* success */);
                default:
                    // 魔法值既不是消息也不是文件结尾，说明后面的数据有问题了
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }
            // 内容
            byte[/*根据消息总大小初始化一个装消息的字节数组*/] bytesContent = new byte[totalSize];
            // crc签名
            int bodyCRC = byteBuffer.getInt();//BODYCRC
            // 队列id
            int queueId = byteBuffer.getInt();//QUEUEID
            // flag
            int flag = byteBuffer.getInt();//FLAG
            long queueOffset = byteBuffer.getLong();//QUEUEOFFSET
            long physicOffset = byteBuffer.getLong();//PHYSICALOFFSET
            int sysFlag = byteBuffer.getInt();//SYSFLAG
            long bornTimeStamp = byteBuffer.getLong();//BORNTIMESTAMP
            ByteBuffer byteBuffer1;//BORNHOST，根据ipv4还是ipv6的不通，长度不同
            if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }
            long storeTimestamp = byteBuffer.getLong();//STORETIMESTAMP
            ByteBuffer byteBuffer2;//STOREHOSTADDRESS，根据ipv4还是ipv6的不通，长度不通
            if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }
            int reconsumeTimes = byteBuffer.getInt();//RECONSUMETIMES
            long preparedTransactionOffset = byteBuffer.getLong();//Prepared Transaction Offset
            int bodyLen = byteBuffer.getInt();// (bodyLength > 0 ? bodyLength : 0) //BODY
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen); //BODY
                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);// 不读 body 则跳过 body 的长度
                }
            }
            byte topicLen = byteBuffer.get(); // topicLength
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);// TOPIC
            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;
            short propertiesLength = byteBuffer.getShort();// 2 + (propertiesLength > 0 ? propertiesLength : 0)
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);// 读取 properties
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);
                // 从属性中拿到 KEYS
                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS/*KEYS*/);
                // 从属性中拿到 UNIQ_KEY
                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX/*UNIQ_KEY*/);
                // 从属性中拿到 TAGS
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS/*TAGS*/);
                if (tags != null && tags.length() > 0) {
                    TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(sysFlag);
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, tags);
                }
                // Timing message processing 定时消息处理
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL/*DELAY*/);
                    if (ScheduleMessageService.SCHEDULE_TOPIC/*SCHEDULE_TOPIC_XXXX*/.equals(topic) && t != null) {
                        // 延迟级别
                        int delayLevel = Integer.parseInt(t);
                        ScheduleMessageService scheduleMessageService = this.defaultMessageStore.getScheduleMessageService();
                        if (delayLevel > scheduleMessageService.getMaxDelayLevel()) {
                            delayLevel = scheduleMessageService.getMaxDelayLevel();
                        }
                        if (delayLevel > 0) {
                            // 如果是定时消息，tagsCode 存储的是 该消息的投递时间
                            tagsCode = scheduleMessageService.computeDeliverTimestamp(delayLevel, storeTimestamp);
                        }
                    }
                }
            }
            int readLength = calMsgLength(sysFlag, bodyLen, topicLen, propertiesLength);
            if (totalSize != readLength/*校验长度是否一致，正常情况是一致的*/) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error("[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}", totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }
            return new DispatchRequest(topic, queueId, physicOffset, totalSize, tagsCode, storeTimestamp, queueOffset, keys, uniqKey, sysFlag, preparedTransactionOffset, propertiesMap);
        } catch (Exception e) {
            // 发生异常，返回失败即可
            // return new DispatchRequest(-1, false /* 失败 */);
        }
        return new DispatchRequest(-1, false /* 失败 */);
    }

    protected static int calMsgLength(int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;
        final int msgLen = 0 +
                +4 //TOTALSIZE（msgLen）
                + 4 //MAGICCODE
                + 4 //BODYCRC
                + 4 //QUEUEID
                + 4 //FLAG
                + 8 //QUEUEOFFSET
                + 8 //PHYSICALOFFSET
                + 4 //SYSFLAG
                + 8 //BORNTIMESTAMP
                + bornhostLength //BORNHOST
                + 8 //STORETIMESTAMP
                + storehostAddressLength //STOREHOSTADDRESS
                + 4 //RECONSUMETIMES
                + 8 //Prepared Transaction Offset
                + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
                + 1 + topicLength //TOPIC
                + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
                + 0;
        return msgLen;
    }

    @Deprecated
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {
        // recover by the minimum time stamp
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();

                if (dispatchRequest.isSuccess()) {
                    // Normal data
                    if (size > 0) {
                        mappedFileOffset += size;

                        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                            if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
                                this.defaultMessageStore.doDispatch(dispatchRequest);
                            }
                        } else {
                            this.defaultMessageStore.doDispatch(dispatchRequest);
                        }
                    }
                    // Come the end of the file, switch to the next file
                    // Since the return 0 representatives met last hole, this can
                    // not be included in truncate offset
                    else if (size == 0) {
                        index++;
                        if (index >= mappedFiles.size()) {
                            // The current branch under normal circumstances should
                            // not happen
                            log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                            break;
                        } else {
                            mappedFile = mappedFiles.get(index);
                            byteBuffer = mappedFile.sliceByteBuffer();
                            processOffset = mappedFile.getFileFromOffset();
                            mappedFileOffset = 0;
                            log.info("recover next physics file, " + mappedFile.getFileName());
                        }
                    }
                } else {
                    log.info("recover physics file end, " + mappedFile.getFileName() + " pos=" + byteBuffer.position());
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        }
        // Commitlog case files are deleted
        else {
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSTION);
        if (magicCode != MESSAGE_MAGIC_CODE) {
            // 魔法值不对，可能被人为损坏了文件，不能恢复
            return false;
        }

        int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;

        // 该文件最后存储消息的时间
        long storeTimestamp = byteBuffer.getLong(msgStoreTimePos);
        if (0 == storeTimestamp) {
            return false;
        }

        MessageStoreConfig messageStoreConfig = this.defaultMessageStore.getMessageStoreConfig();
        StoreCheckpoint storeCheckpoint = this.defaultMessageStore.getStoreCheckpoint();

        // 开启了 index
        boolean indexEnable = messageStoreConfig.isMessageIndexEnable() && messageStoreConfig.isMessageIndexSafe();
        long minTimeInFile = indexEnable ?
                storeCheckpoint.getMinTimestampIndex()/*取 commitLog 文件跟 consumeQueue 文件 跟 indexFile 文件的最后存储时间的最小值*/ :
                storeCheckpoint.getMinTimestamp(); // 取 commitLog 文件跟 consumeQueue 文件的最后存储时间的最小值
        if (storeTimestamp <= minTimeInFile) {
            return true;
        }
        //if (indexEnable) {
        //            long minTimestampIndex = storeCheckpoint.getMinTimestampIndex();
        //            if (storeTimestamp <= minTimestampIndex) {
        //                log.info("find check timestamp, {} {}", storeTimestamp, UtilAll.timeMillisToHumanString(storeTimestamp));
        //                return true;
        //            }
        //        } else {
        //            if (storeTimestamp <= storeCheckpoint.getMinTimestamp()) {
        //                log.info("find check timestamp, {} {}", storeTimestamp, UtilAll.timeMillisToHumanString(storeTimestamp));
        //                return true;
        //            }
        //        }

        return false;
    }

    private void notifyMessageArriving() {

    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    /**
     * @param msg 消息
     * @return
     * @see DefaultMessageStore#asyncPutMessage(org.apache.rocketmq.store.MessageExtBrokerInner) 由该方法调用！
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        msg.setStoreTimestamp(System.currentTimeMillis());
        // Set the message body BODY CRC (consider the most appropriate setting on the client)
        int crc32 = UtilAll.crc32/*计算 body 的CRC 值*/(msg.getBody());
        msg.setBodyCRC(crc32);
        // Back to Results
        AppendMessageResult result;
        // 统计
        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();
        // 消息的主题
        String topic = msg.getTopic();
        // 消息的队列id
        int queueId;
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // 如果是非事务或者是提交事务消息，则进来
            // 可能是重试消息，也可能是用户指定的延迟消息
            if (msg.getDelayTimeLevel() > 0 /* 当前消息是需要延迟的 ，如果用户想发送延迟消息，则在消息中设置该属性即可*/) {
                // Delay Delivery 延迟消息的特殊逻辑
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }
                // SCHEDULE_TOPIC_XXXX
                topic = ScheduleMessageService.SCHEDULE_TOPIC;
                // 修改 queueId,每个延迟级别，就会有多少个延迟队列
                // queueId = delayLevel - 1，因为延迟级别是从1开始，而队列id是从0开始的
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());
                // Backup real topic, queueId
                // 保存2个属性到消息中
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic() /* %RETRY%groupName */);
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()) /* 0 */);
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
                // 修改主题为 SCHEDULE_TOPIC_XXXX
                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        // 持锁时间
        long elapsedTimeInLock = 0;
        // 待释放锁定主题的 mf （lock状态的mf使用的内存会锁死在物理内存中，不会使用swap区，性能很好）
        MappedFile unlockMappedFile = null;
        // 获取当前顺序写的 commitLog 文件
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        putMessageLock.lock/*加锁/获取锁*/(); //spin or ReentrantLock ,depending on store config
        try {
            long beginLockTimestamp/*获取锁的时间*/ = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock/*当 commitLog 开始写入消息的时候，记录开始加锁的时间*/ = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly global -- 这里设置保存时间戳，以保证全局有序
            msg.setStoreTimestamp(beginLockTimestamp/*设置存储时间*/);
            // 获取当前顺序写的 mf
            if (null == mappedFile/*说明 commitlog 目录是空的*/ || mappedFile.isFull()/*文件写满了*/) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0/*传0会创建mf*/); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock/*写消息结束，持锁时间归零*/ = 0;
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED/*创建新 commitLog 文件失败*/, null));
            }

            // 正常情况会继续往下执行

            result = mappedFile.appendMessage/*调用 commitLog 的追加消息方法 */(msg, this.appendMessageCallback);
            AppendMessageStatus appendMessageStatus = result.getStatus();
            switch (appendMessageStatus) {
                case PUT_OK: // 成功
                    break;
                case END_OF_FILE: // 文件尾
                    // 创建新的文件
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message -- 创建一个新文件，重新写入消息
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0/*传0会创建mf*/);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock/*写消息结束，持锁时间归零*/ = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED/*创建新 commitLog 文件失败*/, result));
                    }
                    // 创建新文件成功，继续添加消息进入该文件中
                    result/* TODO 针对这个 result 难道不需要进行这个 switch 了吗？*/ = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock/*写消息结束，持锁时间归零*/ = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                case UNKNOWN_ERROR:
                    beginTimeInLock/*写消息结束，持锁时间归零*/ = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                default:
                    beginTimeInLock/*写消息结束，持锁时间归零*/ = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }

            // 计算加锁的总耗时
            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            // 释放锁
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }
        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()/*预热*/) {
            // 解锁
            this.defaultMessageStore.unlockMappedFile/*TODO 干嘛？？？？*/(unlockMappedFile);
        }
        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);
        //        // Statistics
        //        String msgTopic = msg.getTopic();
        //        AtomicLong topicTimesTotal = storeStatsService.getSinglePutMessageTopicTimesTotal(msgTopic);
        //        topicTimesTotal.incrementAndGet();
        //
        //        AtomicLong topicSizeTotal = storeStatsService.getSinglePutMessageTopicSizeTotal(topic);
        //        // 本次写入的字节数
        //        int wroteBytes = result.getWroteBytes();
        //        topicSizeTotal.addAndGet(wroteBytes);

        // 通知刷盘/落盘服务线程
        CompletableFuture<PutMessageStatus> flushResultFuture = submitFlushRequest(result, putMessageResult, msg);
        // HA 相关的,提交主从复制任务
        CompletableFuture<PutMessageStatus> replicaResultFuture = submitReplicaRequest(result, putMessageResult, msg);
        // 等待2个任务完成再返回
        return flushResultFuture/*先刷盘任务*/.thenCombine(replicaResultFuture/*再主从复制任务*/, (flushStatus/*刷盘任务结果*/, replicaStatus/*主从复制任务结果*/) -> {
            // 2个任务都完成之后，拿到2个结果，在这里统一处理
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
            }
            return putMessageResult;
        });
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //fine-grained lock instead of the coarse-grained
        MessageExtBatchEncoder batchEncoder = batchEncoderThreadLocal.get();

        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));

        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                beginTimeInLock = 0;
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
            }

            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result));
                    }
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                case UNKNOWN_ERROR:
                default:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).addAndGet(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).addAndGet(result.getWroteBytes());

        CompletableFuture<PutMessageStatus> flushOKFuture = submitFlushRequest(result, putMessageResult, messageExtBatch);
        CompletableFuture<PutMessageStatus> replicaOKFuture = submitReplicaRequest(result, putMessageResult, messageExtBatch);
        return flushOKFuture.thenCombine(replicaOKFuture, (flushStatus, replicaStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
            }

            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
            }
            return putMessageResult;
        });

    }

    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        msg.setStoreTimestamp(System.currentTimeMillis());
        // Set the message body BODY CRC (consider the most appropriate setting
        // on the client)
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // Back to Results
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
        int queueId = msg.getQueueId();

        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            if (msg.getDelayTimeLevel() > 0) {
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }

                // 延迟消息先保存到 该主题下
                topic = ScheduleMessageService.SCHEDULE_TOPIC;
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // Backup real topic, queueId
                // 保存真正的主题跟队列id
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                // 保存的时候先保存到延迟消息队列中
                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock = 0;

        MappedFile unlockMappedFile = null;

        // 拿到最后一个文件
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();

            // 锁开始时间
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            msg.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                // 可能会新建
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            // 把该消息写入到该 mappedFile 中去
            result = mappedFile.appendMessage(msg, this.appendMessageCallback);

            // 添加消息结果
            AppendMessageStatus appendMessageStatus = result.getStatus();
            switch (appendMessageStatus) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }

                    // 创建文件之后再次调用添加消息接口
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        // 刷盘逻辑入口
        handleDiskFlush(result, putMessageResult, msg);
        // 主从复制相关
        handleHA(result, putMessageResult, msg);

        return putMessageResult;
    }

    /**
     * @return 返回一个 future 调用线程字节决定是否直接调用 get 从而实现是同步等待还是异步直接返回
     */
    private CompletableFuture<PutMessageStatus> submitFlushRequest(AppendMessageResult result/*写消息结果*/, PutMessageResult putMessageResult/*new PutMessageResult(PutMessageStatus.PUT_OK, result)*/, MessageExt messageExt) {
        // Synchronization flush
        FlushDiskType flushDiskType = this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType();
        if (FlushDiskType.SYNC_FLUSH == flushDiskType) {
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            if (messageExt.isWaitStoreMsgOK()) {

                long nextOffset = result.getWroteOffset() + result.getWroteBytes();
                int syncFlushTimeout = this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();
                GroupCommitRequest request = new GroupCommitRequest(nextOffset, syncFlushTimeout);

                // 提交到刷盘服务中
                service.putRequest(request);

                // 同步刷盘操作也是其他线程操作的，只是需要等待完成
                return request.future();
            } else {
                service.wakeup();
                return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
            }
        }
        // Asynchronous flush
        else {
            // ASYNC_FLUSH 异步刷盘

            boolean transientStorePoolEnable = this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable();

            if (!transientStorePoolEnable) {
                flushCommitLogService.wakeup();
            } else {
                commitLogService.wakeup();
            }

            // 异步刷盘，唤醒刷盘线程之后就直接返回成功
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }
    }

    // 提交副本请求
    public CompletableFuture<PutMessageStatus> submitReplicaRequest(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        /*
         * ASYNC_MASTER,
         * SYNC_MASTER,
         * SLAVE
         */
        BrokerRole brokerRole = this.defaultMessageStore.getMessageStoreConfig().getBrokerRole();

        // 同步复制，需要等待
        if (BrokerRole.SYNC_MASTER == brokerRole) {
            HAService service = this.defaultMessageStore.getHaService();
            if (messageExt.isWaitStoreMsgOK()) {

                long nextOffset = result.getWroteBytes() + result.getWroteOffset();

                if (service.isSlaveOK(nextOffset)) {
                    int syncFlushTimeout = this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();

                    GroupCommitRequest request = new GroupCommitRequest(nextOffset, syncFlushTimeout);

                    // 提交任务
                    service.putRequest(request);

                    WaitNotifyObject waitNotifyObject = service.getWaitNotifyObject();
                    // 唤醒
                    waitNotifyObject.wakeupAll();
                    return request.future();
                } else {
                    return CompletableFuture.completedFuture(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }
        return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
    }

    /**
     * 刷盘逻辑入口
     *
     * @param result 消息append结果
     * @param putMessageResult 消息写入到 commitLog 的结果
     * @param messageExt 消息
     */
    private void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult/*消息写入到 commitLog 的结果*/, MessageExt messageExt) {
        MessageStoreConfig messageStoreConfig = this.defaultMessageStore.getMessageStoreConfig();
        // Synchronization flush
        if (FlushDiskType.SYNC_FLUSH == messageStoreConfig.getFlushDiskType()) {
            // 同步刷盘

            // 获取同步刷盘服务
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            if (messageExt.isWaitStoreMsgOK() /*一般情况返回 true*/) {

                // 需要刷盘到该位点
                long nextOffset = result.getWroteOffset() + result.getWroteBytes();
                GroupCommitRequest request = new GroupCommitRequest(nextOffset  /*当前消息写完之后的commitLog的偏移量或者下条消息的开始偏移量*/);
                /**
                 * 提交到刷盘请求队列
                 *
                 * @see GroupCommitService#requestsWrite
                 */
                service.putRequest(request);

                // 当前写消息线程 试图 获取到 request.future(),当前线程在此阻塞等待
                CompletableFuture<PutMessageStatus> flushOkFuture = request.future();
                PutMessageStatus flushStatus = null;
                try {
                    /**
                     * 写消息线程 试图 获取到 request.future(),当前线程在此阻塞等待，直到刷盘任务完成并且返回，最多阻塞5秒
                     *
                     * @see GroupCommitRequest#wakeupCustomer(boolean) 通过这个方法唤醒当前线程
                     */
                    flushStatus = flushOkFuture.get/*阻塞方法*/(messageStoreConfig.getSyncFlushTimeout()/*5 秒*/, TimeUnit.MILLISECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    //flushOK=false;
                }
                if (flushStatus != PutMessageStatus.PUT_OK) {
                    log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags() + " client address: " + messageExt.getBornHostString());
                    putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }
            } else {
                // 唤醒一下就完事了
                service.wakeup();
            }
        }
        // Asynchronous flush
        else {
            // 异步刷盘
            if (!messageStoreConfig.isTransientStorePoolEnable()) {
                // 唤醒一下就完事了
                // 正常情况下走这里！！！！！！
                flushCommitLogService.wakeup();
            } else {
                // 唤醒一下就完事了
                commitLogService.wakeup();
            }
        }
    }

    public void handleHA(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        MessageStoreConfig messageStoreConfig = this.defaultMessageStore.getMessageStoreConfig();

        /**
         * ASYNC_MASTER, 主节点，异步复制
         * SYNC_MASTER, 主节点，同步复制
         * SLAVE; 从节点
         */
        BrokerRole brokerRole = messageStoreConfig.getBrokerRole();
        if (BrokerRole.SYNC_MASTER == brokerRole) {
            // 只有同步复制的节点，写消息请求才会阻塞

            HAService service = this.defaultMessageStore.getHaService();
            if (messageExt.isWaitStoreMsgOK()) {
                // Determine whether to wait

                long masterPutWhere = result.getWroteOffset() + result.getWroteBytes();
                if (service.isSlaveOK(masterPutWhere)) {
                    GroupCommitRequest request = new GroupCommitRequest(masterPutWhere);
                    service.putRequest(request);
                    WaitNotifyObject waitNotifyObject = service.getWaitNotifyObject();
                    waitNotifyObject.wakeupAll();
                    PutMessageStatus replicaStatus = null;
                    try {
                        CompletableFuture<PutMessageStatus> future = request.future();
                        replicaStatus = future.get(messageStoreConfig.getSyncFlushTimeout()/*5秒*/, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    }
                    if (replicaStatus != PutMessageStatus.PUT_OK) {
                        log.error("do sync transfer other node, wait return, but failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags() + " client address: " + messageExt.getBornHostNameString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }
                }
                // Slave problem
                else {
                    // Tell the producer, slave not available
                    putMessageResult.setPutMessageStatus(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }
    }

    public PutMessageResult putMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //fine-grained lock instead of the coarse-grained
        MessageExtBatchEncoder batchEncoder = batchEncoderThreadLocal.get();

        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));

        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).addAndGet(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).addAndGet(result.getWroteBytes());

        handleDiskFlush(result, putMessageResult, messageExtBatch);

        handleHA(result, putMessageResult, messageExtBatch);

        return putMessageResult;
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    int sysFlag = result.getByteBuffer().getInt(MessageDecoder.SYSFLAG_POSITION);
                    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                    int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
                    return result.getByteBuffer().getLong(msgStoreTimePos);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public boolean appendData(long startOffset, byte[] data) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
        }

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    abstract class FlushCommitLogService extends ServiceThread {

        protected static final int RETRY_TIMES_OVER = 10;
    }

    /**
     * 异步刷盘
     */
    class CommitRealTimeService extends FlushCommitLogService {

        // 打印日志使用
        private long lastCommitTimestamp = 0;

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {

                // 刷盘周期
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

                // 最少刷盘页数
                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

                // 强制刷盘周期
                int commitDataThoroughInterval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    // 达到了强制刷盘周期
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }
                try {
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    if (!result) {
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        //now wake up flush thread.
                        flushCommitLogService.wakeup();
                    }

                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER/*10*/ && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    /**
     * 异步落盘
     */
    class FlushRealTimeService extends FlushCommitLogService {

        private long lastFlushTimestamp = 0;

        private long printTimes = 0;

        @Override
        public void run() {

            /**
             * 循环内的逻辑：
             * 1.读取配置中的刷新间隔时长，线程休息指定时间
             * 2.获取配置中最少刷盘页数，默认是4，只有脏页数据达到指定页数后，才能真正刷盘
             * 3.获取强制刷盘周期，默认是10s，达到强制刷盘周期后，一定会刷盘，不再考虑脏页大小
             * 4.调用外部类 CommitLog.mappedFileQueue.flush()进行刷盘，注意传递参数：int flushPhysicQueueLeastPages(一般4，如果是强制刷盘则传0)
             */
            while (!this.isStopped()/*开启循环,循环直到服务关闭*/) {

                MessageStoreConfig messageStoreConfig = CommitLog.this.defaultMessageStore.getMessageStoreConfig();

                /**
                 * 控制线程休眠方式
                 * true:使用 Sleep休眠
                 * false：使用 countDownLatch.wait(...)休眠
                 * 默认是 false
                 */
                boolean flushCommitLogTimed/* false */ = messageStoreConfig.isFlushCommitLogTimed();
                // 刷盘间隔时间：500
                int interval = messageStoreConfig.getFlushIntervalCommitLog();
                // 刷盘脏页最小值：4
                int flushPhysicQueueLeastPages = messageStoreConfig.getFlushCommitLogLeastPages();
                // 强制刷盘间隔时间：1000 * 10
                int flushPhysicQueueThoroughInterval = messageStoreConfig.getFlushCommitLogThoroughInterval();
                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)/*到了强制刷盘的时间了*/) {
                    // 本次肯定会刷盘，所以把最后一次刷盘的时间设置为当前时间
                    this.lastFlushTimestamp = currentTimeMillis;
                    // 强制刷盘
                    flushPhysicQueueLeastPages = 0;
                }
                try {
                    // 线程休眠方式
                    if (flushCommitLogTimed /* false */) {
                        Thread.sleep(interval/*刷盘间隔时间*/);
                    } else {
                        this.waitForRunning(interval/*刷盘间隔时间*/);
                    }
                    // 刷盘，传入0表示强制刷盘
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages/*刷盘脏页最小值：4*/);
                    // 当前 mfq 中最后一次追加msg的时间
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        // 存储到 checkPoint 文件
                        StoreCheckpoint storeCheckpoint = CommitLog.this.defaultMessageStore.getStoreCheckpoint();
                        storeCheckpoint.setPhysicMsgTimestamp(storeTimestamp);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 执行到这里说明： stopped == true，停机了，跳出上面的 while 循环了！！
            // Normal shutdown, to ensure that all the flush before exit
            // 正常关机，确保退出前 flush 所有数据
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER /*10*/ && !result; i++) {
                // 强制刷盘，保证在停机之前数据都持久化了
                result = CommitLog.this.mappedFileQueue.flush(0/* 强制刷盘 */);
                // TODO 强制刷盘10次也不一定把所有数据都落盘了！！
            }
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    /**
     * 同步刷盘请求
     * 主从复制请求
     */
    public static class GroupCommitRequest {

        // wroteOffset + 消息size
        @Getter
        private final long nextOffset;

        /**
         * 同步逻辑依赖该对象实现
         */
        private CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();

        private final long startTimestamp = System.currentTimeMillis();

        private long timeoutMillis = Long.MAX_VALUE;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.timeoutMillis = timeoutMillis;
        }

        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }

        /**
         * 唤醒在这个 future 上阻塞的线程，这也就是同步刷盘的巧妙设计之处！！！
         *
         * @param flushOK 同步刷盘结果
         */
        public void wakeupCustomer(final boolean flushOK) {
            long endTimestamp = System.currentTimeMillis();

            boolean notTimeout = (endTimestamp - this.startTimestamp) <= this.timeoutMillis;

            boolean flushOkAndNotTimeout = (flushOK && notTimeout);

            PutMessageStatus result = flushOkAndNotTimeout/*刷盘成功并且刷盘没有超时*/ ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT;

            /**
             * 设置结果，这样就会唤醒在这个future 挂起的线程
             * @see CommitLog#handleDiskFlush(org.apache.rocketmq.store.AppendMessageResult, org.apache.rocketmq.store.PutMessageResult, org.apache.rocketmq.common.message.MessageExt)
             */
            this.flushOKFuture.complete/* 唤醒在 future.get() 方法阻塞的线程 */(result);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }

    }

    /**
     * 同步落盘
     * GroupCommit Service
     *
     * @see CommitLog#submitFlushRequest 提交刷盘任务
     */
    class GroupCommitService extends FlushCommitLogService {

        /**
         * 新到的刷盘请求放到这个队列
         *
         * @see GroupCommitService#putRequest(org.apache.rocketmq.store.CommitLog.GroupCommitRequest) 主线程塞进去的
         */
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();

        /**
         * 执行刷盘任务的时候从这个队列中取任务执行
         *
         * 读写分离
         */
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();

        /**
         * @see CommitLog#submitFlushRequest 提交刷盘任务
         * @see CommitLog#handleDiskFlush
         */
        private synchronized void putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {

                /**
                 * 在阻塞的地方往下执行了
                 *
                 * @see GroupCommitService#doCommit() 唤醒之后就执行该方法
                 */
                waitPoint.countDown(); // notify
            }
        }

        private void doCommit() {
            synchronized (this.requestsRead/*读写队列交换之后了*/) {
                if (!this.requestsRead.isEmpty()) {

                    // 检查所有进度
                    // 处理刷盘任务！
                    for (GroupCommitRequest req : this.requestsRead) {
                        // There may be a message in the next file, so a maximum of
                        // two times the flush
                        boolean flushOK = false;
                        for (int i = 0; i < 2/* 每个 GroupCommitRequest 最多循环2次 */ && !flushOK; i++) {

                            // 如果 flushOK 返回 true 则说明，req 关联的"生产者线程"需要被唤醒了，因为它关心的数据已经全部落盘了

                            // 真实刷盘到了什么位置，拿到 commitLog 的刷盘进度
                            long flushedWhere = CommitLog.this.mappedFileQueue.getFlushedWhere();
                            // 该请求要求刷盘到 nextOffset 这个位置
                            long nextOffset = req.getNextOffset();
                            // 如果 真实刷盘位点已经大于等于 该请求要求的刷盘位点，则说明：在这个位点之前的数据都已经刷盘完成了
                            flushOK/* 如果为 true 则说明这个请求要刷盘的位点，已经被刷盘过了，不需要在执行这个刷盘请求了 */ = flushedWhere >= nextOffset;
                            if (!flushOK/* 只有该位点还没有刷到磁盘的情况下才会真正的去刷盘 */) {
                                // 如果上面返回还是没有刷盘，则强制刷盘，刷盘之后会更新 {@link org.apache.rocketmq.store.MappedFileQueue.flushedWhere} 再次循环的时候就会发现该位点已刷盘成功了
                                CommitLog.this.mappedFileQueue.flush(0 /* 0 的意思就是 强制刷盘，只要有没有刷盘的数据就会刷盘，之后上面的 flushOk 肯定会返回 true 自然就退出循环了*/);
                            }
                        }
                        // req 刷盘请求刷盘完成，设置 future 结果
                        // 唤醒同步等待的线程
                        req.wakeupCustomer(flushOK);
                    }

                    // commitLog 刷盘成功之后更新检查点文件
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    // 清理列表，方便后面再次使用，下次交换就会成为 requestsWrite
                    this.requestsRead.clear();
                } else {
                    // Because of individual messages is set to not sync flush, it
                    // will come to this process
                    CommitLog.this.mappedFileQueue.flush(0);
                }
            }
        }

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10); // 休眠 10s 或者 被唤醒 之后就会继续往下执行
                    /**
                     * 在这2行之前会执行方法 {@link GroupCommitService#onWaitEnd()}
                     */
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 停止的时候往下执行

            // Under normal circumstances shutdown, wait for the arrival of the request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }
            synchronized (this) {
                // 没办法了，在这只能自己调
                this.swapRequests();
            }
            this.doCommit();
            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        // 交换 requestsWrite 和 requestsRead 对象
        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    /**
     * @see MappedFile#appendMessagesInner(org.apache.rocketmq.common.message.MessageExt, org.apache.rocketmq.store.AppendMessageCallback)
     */
    class DefaultAppendMessageCallback implements AppendMessageCallback {

        /**
         * File at the end of the minimum fixed length empty
         *
         * 文件结尾最少有8个字节, 剩余空间 + 魔法值
         *
         * @see CommitLog#BLANK_MAGIC_CODE
         */
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;

        private final ByteBuffer msgIdMemory;//16位

        private final ByteBuffer msgIdV6Memory;// 28

        // Store the message content
        @Getter
        private final ByteBuffer msgStoreItemMemory;

        // The maximum length of the message

        /**
         * 4M
         */
        private final int maxMessageSize;

        // Build Message Key
        private final StringBuilder keyBuilder = new StringBuilder();

        private final StringBuilder msgIdBuilder = new StringBuilder();

        DefaultAppendMessageCallback(final int size /* 4M */) {
            this.msgIdMemory = ByteBuffer.allocate(4 + 4 + 8);
            this.msgIdV6Memory = ByteBuffer.allocate(16 + 4 + 8);
            this.msgStoreItemMemory = ByteBuffer.allocate(size/*4M*/ + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        /**
         * 向 commitLog 中追加消息
         * result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
         *
         * @param byteBuffer 一般是 {@link MappedFile#mappedByteBuffer}
         * @param fileFromOffset commitLog文件名,其实就是当前写入文件的起始偏移量
         * @param maxBlank 该文件剩余的可写入的字节数，fileSize - currentPos
         * @param msgInner 消息
         * @return 追加 commitLog 结果
         * @see MappedFile#appendMessagesInner(org.apache.rocketmq.common.message.MessageExt, org.apache.rocketmq.store.AppendMessageCallback)
         * @see CommitLog#topicQueueTable 该缓存也在这里更新
         */
        public AppendMessageResult doAppend(
                final long fileFromOffset/*commitLog文件名,其实就是当前写入文件的起始偏移量*/, final ByteBuffer byteBuffer/*当前写入mappedFile的mappedByteBuffer*/, final int maxBlank/*该文件剩余的可写入的字节数，fileSize - currentPos*/, final MessageExtBrokerInner msgInner) {

            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // PHY OFFSET
            // 计算得到这条消息的物理 offset
            long wroteOffset = fileFromOffset + byteBuffer.position()/*byteBuffer有内容的地方*/;

            int sysflag = msgInner.getSysFlag();

            // 生成消息主机的ip地址长度，如果是ipv4,长度是8，如果是ipv6，长度是20
            int bornHostLength = (sysflag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            // 保存消息的主机ip长度，如果是ipv4,长度是8，如果是ipv6，长度是20
            int storeHostLength = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;

            // 缓存 发送消息机器 ip
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            // 缓存 存储ip
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);
            // TODO ??
            this.resetByteBuffer(storeHostHolder, storeHostLength);
            // 创建消息 id
            String msgId;

            /**
             * @see MessageExt#socketAddress2ByteBuffer(java.net.SocketAddress, java.nio.ByteBuffer)
             */
            if ((sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                // 把 生成 消息的 storeHost 存储到 storeHostHolder 并且返回
                ByteBuffer storeHostBytes = msgInner.getStoreHostBytes(storeHostHolder);
                msgId = MessageDecoder.createMessageId(this.msgIdMemory, storeHostBytes, wroteOffset);
            } else {
                ByteBuffer storeHostBytes = msgInner.getStoreHostBytes(storeHostHolder);
                msgId = MessageDecoder.createMessageId(this.msgIdV6Memory, storeHostBytes, wroteOffset);
            }

            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            String msgInnerTopic = msgInner.getTopic();
            keyBuilder.append(msgInnerTopic);
            keyBuilder.append('-');
            int innerQueueId = msgInner.getQueueId();
            keyBuilder.append(innerQueueId);

            // topic-queueId
            String key = keyBuilder.toString();

            // HashMap<String/* topic-queueid */, Long/* 该队列的逻辑offset */> topicQueueTable
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                // 首次写入消息到该主题下的该队列
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            // Transaction messages that require special handling
            final int tranType/*事务类型*/ = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the
                // consumer queuec
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            /**
             * Serialize message
             */
            String propertiesString = msgInner.getPropertiesString();
            final byte[] propertiesData = propertiesString == null ? null : propertiesString.getBytes(MessageDecoder.CHARSET_UTF8);
            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;

            byte[] body = msgInner.getBody();
            final int bodyLength = body == null ? 0 : body.length;

            // 计数消息长度
            /**
             * final int msgLen =
             *                   4 //TOTALSIZE
             *                 + 4 //MAGICCODE
             *                 + 4 //BODYCRC
             *                 + 4 //QUEUEID
             *                 + 4 //FLAG
             *                 + 8 //QUEUEOFFSET
             *                 + 8 //PHYSICALOFFSET
             *                 + 4 //SYSFLAG
             *                 + 8 //BORNTIMESTAMP
             *                 + bornhostLength //BORNHOST
             *                 + 8 //STORETIMESTAMP
             *                 + storehostAddressLength //STOREHOSTADDRESS
             *                 + 4 //RECONSUMETIMES
             *                 + 8 //Prepared Transaction Offset
             *                 + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
             *                 + 1 + topicLength //TOPIC
             *                 + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
             *                 + 0;
             */
            final int msgLen = calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

            // Exceeds the maximum message
            if (msgLen > this.maxMessageSize/*该消息大小超过了最大值4M*/) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // Determines whether there is sufficient free space
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH/*文件结尾最少有8个字节, 剩余空间 + 魔法值*/) > maxBlank/*说明到文件结尾了*/) {
                // 说明这个文件即将写满

                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank/*文件最后剩余多少字节*/);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE)/*文件结尾的魔法值*/;
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);

                // 返回结果表示已经到文件尾
                // 拿到结果之后判断，如果发现是文件尾了，则新建文件再次写入新文件
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            // 还没到文件尾，这条消息可以写入

            /**
             *   4 //TOTALSIZE
             * + 4 //MAGICCODE
             * + 4 //BODYCRC
             * + 4 //QUEUEID
             * + 4 //FLAG
             * + 8 //QUEUEOFFSET
             * + 8 //PHYSICALOFFSET
             * + 4 //SYSFLAG
             * + 8 //BORNTIMESTAMP
             * + bornhostLength //BORNHOST
             * + 8 //STORETIMESTAMP
             * + storehostAddressLength //STOREHOSTADDRESS
             * + 4 //RECONSUMETIMES
             * + 8 //Prepared Transaction Offset
             * + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
             * + 1 + topicLength //TOPIC
             * + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
             * + 0;
             */
            // Initialization of storage space
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            // 1 TOTALSIZE
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE
            this.msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID
            this.msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG
            this.msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
            // 8 SYSFLAG
            this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST
            this.resetByteBuffer(bornHostHolder, bornHostLength);
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes(bornHostHolder));
            // 11 STORETIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS
            this.resetByteBuffer(storeHostHolder, storeHostLength);
            this.msgStoreItemMemory.put(msgInner.getStoreHostBytes(storeHostHolder));
            // 13 RECONSUMETIMES
            this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0) {
                this.msgStoreItemMemory.put(msgInner.getBody());
            }
            // 16 TOPIC
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0) {
                this.msgStoreItemMemory.put(propertiesData);
            }

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            // Write messages to the queue buffer
            byteBuffer.put/*写入到 commitLog 文件的虚拟内存中*/(this.msgStoreItemMemory.array(), 0, msgLen);

            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId, msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);

            switch (tranType) {
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    CommitLog.this.topicQueueTable.put(key, ++queueOffset/*该队列添加了一条消息，则逻辑偏移量++*/);
                    break;
                default:
                    break;
            }
            return result;
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank, final MessageExtBatch messageExtBatch) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            keyBuilder.append(messageExtBatch.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(messageExtBatch.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;
            msgIdBuilder.setLength(0);
            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            int sysFlag = messageExtBatch.getSysFlag();
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            this.resetByteBuffer(storeHostHolder, storeHostLength);
            ByteBuffer storeHostBytes = messageExtBatch.getStoreHostBytes(storeHostHolder);
            messagesByteBuff.mark();
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();
                final int bodyLen = msgLen - 40; //only for log, just estimate it
                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                            + ", maxMessageSize: " + this.maxMessageSize);
                    return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
                }
                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.resetByteBuffer(this.msgStoreItemMemory, 8);
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdBuilder.toString(), messageExtBatch.getStoreTimestamp(),
                            beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                messagesByteBuff.position(msgPos + 20);
                messagesByteBuff.putLong(queueOffset);
                messagesByteBuff.putLong(wroteOffset + totalMsgLen - msgLen);

                storeHostBytes.rewind();
                String msgId;
                if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                    msgId = MessageDecoder.createMessageId(this.msgIdMemory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                } else {
                    msgId = MessageDecoder.createMessageId(this.msgIdV6Memory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                }

                if (msgIdBuilder.length() > 0) {
                    msgIdBuilder.append(',').append(msgId);
                } else {
                    msgIdBuilder.append(msgId);
                }
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdBuilder.toString(),
                    messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);
            CommitLog.this.topicQueueTable.put(key, queueOffset);

            return result;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    public static class MessageExtBatchEncoder {

        // Store the message content
        private final ByteBuffer msgBatchMemory;

        // The maximum length of the message
        private final int maxMessageSize;

        MessageExtBatchEncoder(final int size) {
            this.msgBatchMemory = ByteBuffer.allocateDirect(size);
            this.maxMessageSize = size;
        }

        public ByteBuffer encode(final MessageExtBatch messageExtBatch) {
            msgBatchMemory.clear(); //not thread-safe
            int totalMsgLen = 0;
            ByteBuffer messagesByteBuff = messageExtBatch.wrap();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                messagesByteBuff.getInt();
                // 2 MAGICCODE
                messagesByteBuff.getInt();
                // 3 BODYCRC
                messagesByteBuff.getInt();
                // 4 FLAG
                int flag = messagesByteBuff.getInt();
                // 5 BODY
                int bodyLen = messagesByteBuff.getInt();
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 6 properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);

                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

                final int topicLength = topicData.length;

                final int msgLen = calMsgLength(messageExtBatch.getSysFlag(), bodyLen, topicLength, propertiesLen);

                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                            + ", maxMessageSize: " + this.maxMessageSize);
                    throw new RuntimeException("message size exceeded");
                }

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if (totalMsgLen > maxMessageSize) {
                    throw new RuntimeException("message size exceeded");
                }

                // 1 TOTALSIZE
                this.msgBatchMemory.putInt(msgLen);
                // 2 MAGICCODE
                this.msgBatchMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                this.msgBatchMemory.putInt(bodyCrc);
                // 4 QUEUEID
                this.msgBatchMemory.putInt(messageExtBatch.getQueueId());
                // 5 FLAG
                this.msgBatchMemory.putInt(flag);
                // 6 QUEUEOFFSET
                this.msgBatchMemory.putLong(0);
                // 7 PHYSICALOFFSET
                this.msgBatchMemory.putLong(0);
                // 8 SYSFLAG
                this.msgBatchMemory.putInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getBornTimestamp());
                // 10 BORNHOST
                this.resetByteBuffer(bornHostHolder, bornHostLength);
                this.msgBatchMemory.put(messageExtBatch.getBornHostBytes(bornHostHolder));
                // 11 STORETIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                this.resetByteBuffer(storeHostHolder, storeHostLength);
                this.msgBatchMemory.put(messageExtBatch.getStoreHostBytes(storeHostHolder));
                // 13 RECONSUMETIMES
                this.msgBatchMemory.putInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset, batch does not support transaction
                this.msgBatchMemory.putLong(0);
                // 15 BODY
                this.msgBatchMemory.putInt(bodyLen);
                if (bodyLen > 0) {
                    this.msgBatchMemory.put(messagesByteBuff.array(), bodyPos, bodyLen);
                }
                // 16 TOPIC
                this.msgBatchMemory.put((byte) topicLength);
                this.msgBatchMemory.put(topicData);
                // 17 PROPERTIES
                this.msgBatchMemory.putShort(propertiesLen);
                if (propertiesLen > 0) {
                    this.msgBatchMemory.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
                }
            }
            msgBatchMemory.flip();
            return msgBatchMemory;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }
}

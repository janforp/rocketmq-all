package org.apache.rocketmq.store.index;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.RunningFlags;
import org.apache.rocketmq.store.StoreCheckpoint;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @see DefaultMessageStore.CommitLogDispatcherBuildIndex
 */
public class IndexService {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * Maximum times to attempt index file creation.
     */
    private static final int MAX_TRY_IDX_CREATE = 3;

    private final DefaultMessageStore defaultMessageStore;

    // 配置，默认5000000，每个索引文件包含的 hash 桶数量
    private final int hashSlotNum;

    // 配置，默认5000000 * 4 ，每个索引文件包含的 索引条目 数量
    private final int indexNum;

    // 索引文件存储目录，默认是 ：System.getProperty("user.home") + File.separator + "store";
    private final String storePath;/* /Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/index/  */

    // 索引文件列表
    private final ArrayList<IndexFile/*一个索引文件*/> indexFileList = new ArrayList<>();

    // 操作 indexFileList 的锁
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        // 配置，默认5000000，每个索引文件包含的 hash 桶数量
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        // 配置，默认5000000 * 4 ，每个索引文件包含的 索引条目 数量
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        // 索引文件存储目录，默认是 ：System.getProperty("user.home") + File.separator + "store";
        this.storePath = StorePathConfigHelper.getStorePathIndex(store.getMessageStoreConfig().getStorePathRootDir());
    }

    /**
     * @param lastExitOK 上次是否飞正常退出？？？？
     */
    public boolean load(final boolean lastExitOK) {
        File dir = new File(this.storePath /* /Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/index/ */);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);

            StoreCheckpoint storeCheckpoint = this.defaultMessageStore.getStoreCheckpoint();

            for (File file : files) {
                try {

                    String path = file.getPath();/*/Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/index/20220119155631210*/
                    IndexFile f = new IndexFile(path, this.hashSlotNum, this.indexNum, 0, 0);

                    // 加载，恢复 headerIndex
                    f.load();

                    if (!lastExitOK) {
                        // 上次是否正常退出？？？？
                        if (f.getEndTimestamp()/*加载完成之后就能拿到该值*/ > storeCheckpoint.getIndexMsgTimestamp()) {

                            // 这个索引文件可能是损坏的
                            f.destroy(0);
                            continue;
                        }
                    }

                    log.info("load index file OK, " + f.getFileName());
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    log.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    log.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }

    /**
     * @param offset commitLog 目录中的的最小的 offset(其实也是最在的msg 的偏移量)
     */
    public void deleteExpiredFile(long offset/* commitLog 目录中的的最小（也就是最早的）的 offset */) {
        Object[] files = null;
        try {
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }

            // 拿到第一个索引文件的结尾的消息的偏移量，因为第一个文件中的数据肯定是最早的呀！！！！
            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            if (endPhyOffset < offset) {
                // 说明索引目录内存在过期的索引文件
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        if (files != null) {
            // 执行删除逻辑

            // 待删除列表
            List<IndexFile> fileList = new ArrayList<>();

            // 遍历，但是肯定保存最后一个索引文件（files.length - 1）
            for (int i = 0; i < (files.length - 1)/*肯定要保留最后一个索引文件*/; i++) {
                IndexFile f = (IndexFile) files[i];
                if (f.getEndPhyOffset() < offset) {
                    // 添加到待删除列表
                    fileList.add(f);
                } else {
                    break;
                }
            }

            this.deleteExpiredFile(fileList);
        }
    }

    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (IndexFile file : files) {
                    boolean destroyed = file.destroy(3000);
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            log.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        List<Long> phyOffsets = new ArrayList<>(maxNum);
        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;

        int maxMsgsNumBatch = this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch();/*64*/
        maxNum = Math.min(maxNum, maxMsgsNumBatch/*64*/);
        try {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {

                // 从尾开始往前遍历，因为用户大概率关心的是最近的数据！！
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    // 拿到索引文件
                    IndexFile f = this.indexFileList.get(i - 1);

                    // 是否是最后一个文件
                    boolean lastFile = i == this.indexFileList.size();
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }

                    if (f.isTimeMatched(begin, end)) {
                        // 时间能够满足，则去该文件查询，并且把结果存储到集合 phyOffsets 中
                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end, lastFile);
                    }

                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }

                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    private String buildKey(final String topic, final String key) {
        return topic + "#" + key;
    }

    /**
     * 上层 DefaultMessageStore 内存启动的异步线程会将 commitLog 内的新 msg 包装成 DispatchRequest 对象
     * 最终交给当前方法
     *
     * @param req 封装了一条msg
     * @see DefaultMessageStore.CommitLogDispatcherBuildIndex#dispatch(org.apache.rocketmq.store.DispatchRequest)
     */
    public void buildIndex(DispatchRequest req /* 其实就是一条消息，只是没有 body 而已*/) {
        // 获取当前索引文件，如果 list 内不存在文件 或者当前file写满了，则创建新的 file并返回
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {
            // 拿到索引文件最后一条消息的 偏移量
            long endPhyOffset = indexFile.getEndPhyOffset();
            // 消息主题
            String topic = req.getTopic();
            // 消息 keys
            String keys = req.getKeys();
            if (req.getCommitLogOffset()/*消息的偏移量*/ < endPhyOffset) {
                // 如果当前消息的提交偏移量 小于 文件的最后一个 偏移量，则说明当前消息已经提交到索引了
                // 则不需要重复提交索引了
                return;
            }

            final int tranType = MessageSysFlag.getTransactionValue(req.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }

            // 系统唯一索引，为消息创建唯一索引
            String reqUniqKey = req.getUniqKey();
            if (reqUniqKey != null) {
                String key/*主题+索引key*/ = buildKey(topic, reqUniqKey);
                indexFile = putKey(indexFile, req, key);
                if (indexFile == null) {
                    log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), reqUniqKey);
                    return;
                }
            }

            // 自定义 keys 创建索引
            if (keys != null && keys.length() > 0) {

                // 按空格分开
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                for (String key : keyset) {
                    if (key.length() > 0) {
                        String buildKey = buildKey(topic, key);
                        indexFile = putKey(indexFile, req, buildKey);
                        if (indexFile == null) {
                            log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), reqUniqKey);
                            return;
                        }
                    }
                }
            }
        } else {
            log.error("build index error, stop building index");
        }
    }

    /**
     * @param indexFile 当前索引文件
     * @param msg 当前消息
     * @param idxKey 当前消息需要创建索引的 key
     * @return 索引文件
     */
    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
            log.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");

            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }

            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }

        return indexFile;
    }

    /**
     * Retries to get or create index file.
     *
     * @return {@link IndexFile} or null on failure.
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        for (int times = 0; times < MAX_TRY_IDX_CREATE/*3*/; times++) {
            indexFile = this.getAndCreateLastIndexFile();
            if (null != indexFile) {
                break;
            }

            try {
                log.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }

        if (null == indexFile) {
            RunningFlags accessRights = this.defaultMessageStore.getAccessRights();
            accessRights.makeIndexFileError();
            log.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    private IndexFile getAndCreateLastIndexFile() {
        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;

        {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {

                // 最后一个文件
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                if (!tmp.isWriteFull() /* 最后一个索引文件是否写满了 */) {

                    // 没写满，则返回最后一个即可
                    indexFile = tmp;
                } else {

                    // 写满了
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                    prevIndexFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }

        // 创建新的文件（没有任何文件 或者 最后一个文件写满了 都会创建新文件！！！）
        if (indexFile == null) {
            try {

                // /Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/index/20220119155631210
                String fileName = this.storePath + File.separator + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                indexFile = new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset, lastUpdateIndexTimestamp);
                this.readWriteLock.writeLock().lock();
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                log.error("getLastIndexFile exception ", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }

            if (indexFile != null) {

                // 把上一个写满的文件刷盘
                final IndexFile flushThisFile = prevIndexFile;
                // 如果是因为上一个文件写满了而创建新文件，则需要把上一个文件刷盘
                Thread flushThread = new Thread(() -> IndexService.this.flush(flushThisFile), "FlushIndexFileThread");
                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

    public void flush(final IndexFile f) {
        if (null == f) {
            return;
        }

        long indexMsgTimestamp = 0;

        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }

        f.flush();

        StoreCheckpoint storeCheckpoint = this.defaultMessageStore.getStoreCheckpoint();
        if (indexMsgTimestamp > 0) {
            storeCheckpoint.setIndexMsgTimestamp(indexMsgTimestamp);
            storeCheckpoint.flush();
        }
    }

    public void start() {

    }

    public void shutdown() {

    }
}

package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 异步创建 mappedFile 的服务
 *
 * Create MappedFile in advance
 */
public class AllocateMappedFileService extends ServiceThread {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final ConcurrentMap<String/*filePath 全路径*/, AllocateRequest/*filePath 创建的请求*/> requestTable = new ConcurrentHashMap<>();

    // 优先队列，存放请求
    private final PriorityBlockingQueue<AllocateRequest> requestQueue = new PriorityBlockingQueue<>();

    private volatile boolean hasException = false;

    private final DefaultMessageStore messageStore;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * 提交创建任务的接口，该方法会阻塞，直到第一个文件创建成功
     *
     * 该方法创建的文件 大于等于1g 的是会预热
     *
     * @param nextFilePath 全路径
     * @param nextNextFilePath 全路径
     * @param fileSize 大小
     * @return 结果
     */
    public MappedFile putRequestAndReturnMappedFile(String nextFilePath/*全路径*/, String nextNextFilePath/*全路径*/, int fileSize) {
        int canSubmitRequests = 2;
        MessageStoreConfig messageStoreConfig = this.messageStore.getMessageStoreConfig();
        BrokerRole brokerRole = messageStoreConfig.getBrokerRole();
        if (messageStoreConfig.isTransientStorePoolEnable()) {
            if (messageStoreConfig.isFastFailIfNoBufferInStorePool() && BrokerRole.SLAVE != brokerRole) {
                /*//if broker is slave, don't fast fail even no buffer in pool*/

                // 已经初始化过的对象
                TransientStorePool transientStorePool = this.messageStore.getTransientStorePool();
                canSubmitRequests = transientStorePool.availableBufferNums()/*剩余的缓存数量*/ - this.requestQueue.size()/*当前请求的数量，也就是要创建的文件数量*/;
            }
        }

        // 把请求封装成一个请求大小
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);

        //  ConcurrentMap<String/*filePath 全路径*/, AllocateRequest/*filePath 创建的请求*/> requestTable
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null/*返回null，说明不是重复的请求*/;

        if (nextPutOK/*这次是一个新的请求*/) {
            if (canSubmitRequests <= 0) {
                // 内存池不够了
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " + "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());

                //  ConcurrentMap<String/*filePath 全路径*/, AllocateRequest/*filePath 创建的请求*/> requestTable
                this.requestTable.remove(nextFilePath);
                return null;
            }
            // 放到优先队列中
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("never expected here, add a request to preallocate queue failed");
            }

            // 可以提交的请求数量减一个
            canSubmitRequests--;
        }

        // 再次新建一个请求
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);

        //  ConcurrentMap<String/*filePath 全路径*/, AllocateRequest/*filePath 创建的请求*/> requestTable
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null/*返回null，说明不是重复的请求*/;
        if (nextNextPutOK/*这次是一个新的请求*/) {
            if (canSubmitRequests <= 0) {
                String msg = "[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " + "RequestQueueSize : {}, StorePoolSize: {}";
                log.warn(msg, this.requestQueue.size(), this.messageStore.getTransientStorePool().availableBufferNums());
                this.requestTable.remove(nextNextFilePath);
            } else {
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
            }
        }

        /**
         * @see AllocateMappedFileService#mmapOperation() 这个操作可能会发生异常
         */
        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                int waitTimeOut = 1000 * 5;
                CountDownLatch countDownLatch = result.getCountDownLatch();

                // 等待 nextFilePath 文件的创建 5 秒
                boolean waitOK = countDownLatch.await(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!waitOK) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                    return null;
                } else {
                    this.requestTable.remove(nextFilePath);

                    // 返回 nextFilePath 创建的结果
                    return result.getMappedFile();
                }
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    @Override
    public void shutdown() {
        super.shutdown(true);

        // ConcurrentMap<String/*filePath 全路径*/, AllocateRequest/*filePath 创建的请求*/> requestTable
        Collection<AllocateRequest/*filePath 创建的请求*/> allocateRequests = this.requestTable.values();
        for (AllocateRequest req : allocateRequests) {
            if (req.mappedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mappedFile.getFileName());
                req.mappedFile.destroy(1000);
            }
        }
    }

    /**
     * 基于优先级队列工作
     */
    @SuppressWarnings("all")
    public void run() {
        log.info(this.getServiceName() + " service started");
        while (!this.isStopped() && this.mmapOperation() /*执行操作的函数是这个！该函数 只有被外线程中断，才会返回false，意思就是会一直循环*/) {
            // 循环创建，直到成功
        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * 该方法在另外一个线程中执行的，所以，在从任务队列中获取任务的时候阻塞也没关系
     * Only interrupted by the external thread, will return false
     * 只有被外线程中断，才会返回false
     *
     * 这里不断循环创建用户想要创建的文件
     */
    @SuppressWarnings("all")
    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        MessageStoreConfig messageStoreConfig = messageStore.getMessageStoreConfig();
        try {
            // 拿到一个请求，如果没有请求则再次阻塞
            // 该方法在另外一个线程中执行的，所以，在从任务队列中获取任务的时候阻塞也没关系！！！！！
            req = this.requestQueue.take();
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            if (null == expectedRequest) {

                // 超时了
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " " + req.getFileSize());
                return true;
            }
            if (expectedRequest != req) {
                log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " " + req.getFileSize() + ", req:" + req + ", expectedRequest:" + expectedRequest);
                return true;
            }

            if (req.getMappedFile() == null) {
                long beginTime = System.currentTimeMillis();

                // 该创建文件对应的结果
                MappedFile mappedFile;

                if (messageStoreConfig.isTransientStorePoolEnable()) {
                    TransientStorePool transientStorePool = messageStore.getTransientStorePool();
                    try {
                        ServiceLoader<MappedFile> load = ServiceLoader.load(MappedFile.class);
                        Iterator<MappedFile> mappedFileIterator = load.iterator();
                        mappedFile = mappedFileIterator.next();
                        mappedFile.init(req.getFilePath(), req.getFileSize(), transientStorePool);
                    } catch (RuntimeException e) {
                        log.warn("Use default implementation.");
                        mappedFile = new MappedFile(req.getFilePath(), req.getFileSize(), transientStorePool);
                    }
                } else {

                    // 创建mf对象
                    mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                }

                // 花里胡哨的 傻逼东西，这么写有说明意义？
                long elapsedTime = UtilAll.computeElapsedTimeMilliseconds(beginTime);
                if (elapsedTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) " + elapsedTime + " queue size " + queueSize + " " + req.getFilePath() + " " + req.getFileSize());
                }

                // pre write mappedFile
                if (mappedFile.getFileSize() >= messageStoreConfig.getMappedFileSizeCommitLog() && messageStoreConfig.isWarmMapedFileEnable()) {
                    // 预热
                    FlushDiskType flushDiskType = messageStoreConfig.getFlushDiskType();
                    // 1024 / 4 * 16 = 16
                    int flushLeastPagesWhenWarmMapedFile = messageStoreConfig.getFlushLeastPagesWhenWarmMapedFile();
                    /**
                     * 预热，会真实的在内存中开辟空间，避免发生缺页异常
                     */
                    mappedFile.warmMappedFile(flushDiskType, flushLeastPagesWhenWarmMapedFile);
                }

                // 把结果设置到请求体中去
                req.setMappedFile(mappedFile);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            this.hasException = true;
            // 线程被中断，返回 false
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
            if (null != req) {
                // 再次放进去
                requestQueue.offer(req);
                try {
                    // 睡眠一秒之后，继续循环
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                    // 忽略异常，继续循环
                }
            }
        } finally {
            if (req != null && isSuccess) {
                // 唤醒阻塞的线程
                CountDownLatch countDownLatch = req.getCountDownLatch();
                countDownLatch.countDown();
            }
        }
        return true;
    }

    static class AllocateRequest implements Comparable<AllocateRequest> {

        // Full file path       全路径
        @Getter
        @Setter
        private String filePath;

        // 要创建文件的大小
        @Getter
        @Setter
        private int fileSize;

        @Getter
        @Setter
        private CountDownLatch countDownLatch = new CountDownLatch(1);

        // 保存结果的
        @Getter
        @Setter
        private volatile MappedFile mappedFile = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize) {
                return 1;
            } else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                int mIndex = this.filePath.lastIndexOf(File.separator);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(File.separator);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                return Long.compare(mName, oName);
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
            result = prime * result + fileSize;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            AllocateRequest other = (AllocateRequest) obj;
            if (filePath == null) {
                if (other.filePath != null) {
                    return false;
                }
            } else if (!filePath.equals(other.filePath)) {
                return false;
            }
            return fileSize == other.fileSize;
        }
    }
}

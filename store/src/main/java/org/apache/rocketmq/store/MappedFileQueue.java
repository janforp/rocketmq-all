package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * MappedFile 的管理对象，MappedFile容器
 *
 * commitLog跟 conusmeQueue 都通过这个对象来控制
 */
@SuppressWarnings("all")
public class MappedFileQueue {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private static final int DELETE_FILES_BATCH_MAX = 10;

    // 目录

    /**
     * 当前 MappedFileQueue 对象管理的文件所在的目录
     * CommitLog:.../store/commitLog
     * ConsumerQueue: ../store/xxx_topci/0
     */
    private final String storePath;

    // 如果管理的是 MappedFile 则是 1G,如果是consumerQueue则为六百万字节
    @Getter
    private final int mappedFileSize;

    /**
     * 维护了多个文件，每个文件都有一个对象
     * 该目录下的每个文件
     */
    @Getter
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    // 创建新 MappedFile 的服务，内部有自己的线程，我们通过向他提交请求，它内部线程处理完后会返回给我们结果，结果就是 MappedFile 对象
    private final AllocateMappedFileService allocateMappedFileService;

    // 目录下的刷盘位点，其实就是：curMappedFile.fileName + curMappedFile.wrotePos
    @Setter
    @Getter
    private long flushedWhere = 0;

    // 一般不用这个
    @Setter
    @Getter
    private long committedWhere = 0;

    // 当前目录最后一条消息的存储时间
    @Getter
    private volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize, AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath; // 目录
        this.mappedFileSize = mappedFileSize; // 文件大小
        this.allocateMappedFileService = allocateMappedFileService; // 服务对象
    }

    public void checkSelf() {
        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}", pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /**
     * 查询当前文件列表中，修改时间 >= 传入时间的第一个文件
     *
     * @param timestamp 修改时间
     * @return 当前文件列表中，修改时间 >= 传入时间的第一个文件
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs) {
            return null;
        }

        for (Object mf : mfs) {
            MappedFile mappedFile = (MappedFile) mf;

            // 当前文件的最后修改时间
            long lastModifiedTimestamp = mappedFile.getLastModifiedTimestamp();
            if (lastModifiedTimestamp >= timestamp) {
                return mappedFile;
            }
        }

        // 如果都不满足，则返回最后一个文件
        return (MappedFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    /**
     * 删除脏文件，删除
     *
     * 意思就是删除 最小偏移量在 offset 之后的文件，因为后面可能是脏文件，出问题了
     *
     *
     * 0  .......  n
     * n+1......... 2n
     * n+2 ......... 3n
     * ...
     * m .......... mn
     *
     * 传入的 offset 是 n+10
     *
     * 则删除 n+2 以及之后的文件剩余文件为
     * 0  .......  n
     * n+1......... 2n
     *
     * @param offset 物理偏移量
     */
    public void truncateDirtyFiles(long offset) {

        // 待删除的列表
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        // 遍历所有的文件
        for (MappedFile file : this.mappedFiles) {

            // 该文件的最后一个字节的偏移量
            long fileTailOffset = file.getFileFromOffset()/*文件名称转long，*/ + this.mappedFileSize/*每个文件的大小*/;
            if (fileTailOffset > offset  /*如果该文件的最大偏移量 > 传入的offset*/) {
                // 存在下面2个情况：
                // fileFromOffset ..................................... offset ............................. fileTailOffset
                // ......... offset .......上一个文件的结尾   fileFromOffset .................................................................. fileTailOffset

                if (offset >= file.getFileFromOffset()) {
                    // 情况1，传入的 offset 被包含在当前文件中
                    // fileFromOffset ..................................... offset ............................. fileTailOffset

                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    // 情况2，传入的 offset 在当前文件之前了
                    // ......... offset .......上一个文件的结尾   fileFromOffset .................................................................. fileTailOffset

                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }

            // 情况3
            // fileFromOffset .................................................................. fileTailOffset 文件尾巴  ..... offset ....
            // 如果文件的最大偏移量 小于 传入的 offset 则该文件不动
        }

        this.deleteExpiredFile(willRemoveFiles);
    }

    void deleteExpiredFile(List<MappedFile> files/*willRemoveFiles*/) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    /**
     * Broker启动阶段，加载本地磁盘数据使用，该方法获取"storePath"目录下的文件，创建对应的 MappedFile 对象并加入到list内
     *
     * @return 成功失败
     */
    public boolean load() {
        // 拿到当前目录
        // /Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/consumequeue 目录
        File dir = new File(this.storePath);
        /*
         * 拿到目录下的所有文件
         *
         * 主题为 order 的文件夹：/Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/consumequeue/order
         * 主题为 TopicTest 的文件夹：/Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/consumequeue/TopicTest
         */
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            // 安装文件名称排序
            Arrays.sort(files);
            for (File file : files) {
                // 遍历每个主题下面的各个队列的文件夹

                // 理论上说，当前目录下的每个文件大小都是 mappedFileSize
                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length() + " length not matched message store config value, please check it manually");
                    return false;
                }

                try {
                    // 根据当前文件的路径创建对象
                    String filePath = file.getPath();
                    MappedFile mappedFile = new MappedFile(filePath, mappedFileSize);

                    // 设置位点
                    // 这里都不是准确值，准确值需要在 recover 节点设置
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);

                    // 加入集合
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + filePath + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty()) {
            return 0;
        }

        // 刷盘位点
        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * @param startOffset 文件起始偏移量
     * @param needCreate 是否需要创建
     * @return 对象
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {

        // 该值控制是否需要创建 mappedFile ，当需要创建 mappedFile 的时候，它充当文件名的结尾
        // 两种情况会创建
        // 1.list 内没有 mappedFile
        // 2.list 中最后一个 mappedFile (当前顺序写的对象)已经写满了
        long createOffset = -1;

        // 最后一个文件
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast == null) {
            // 情况1：list 内没有 mappedFile

            // createOffset 取值必须是 mappedFileSize 倍数或者 0

            /**
             * 举例：
             * mappedFileSize = 100 的时候
             * 1. startOffset = 101， l1 = 101 % 100 = 1,    则 createOffset = 101 - 1 = 100
             * 2. startOffset = 330， l1 = 330 % 100 = 30，   则 createOffset = 330 - 30 = 300
             */
            long l = startOffset % this.mappedFileSize;
            createOffset = startOffset - l;
        }

        if (mappedFileLast != null && mappedFileLast.isFull()) {
            // 情况2.list 中最后一个 mappedFile (当前顺序写的对象)已经写满了

            // createOffset 为上一个文件名 + mappedFileSize
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            // 真正需要创建新的 mappedFile

            // 待创建文件的绝对路径（storePath/）如：../store/commitlog/00000000000012345567
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            // 每次预创建2个文件
            // 获取下下次的文件绝对路径 如：../store/commitlog/0000000000002234231231
            String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset + this.mappedFileSize);

            // 即将创建的对象
            MappedFile mappedFile = null;

            if (this.allocateMappedFileService != null) {
                // 如果该服务不为空，则使用该服务去创建 mappedFile 对象
                // 使用该对象创建的好处：当 mappedFileSize >= 1g 的时候，该服务会执行它的预热方法，在物理内存上真正的创建空间，后面通过 mappedFile 写的时候速度会快些
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath, nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    // 自己创建
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            if (mappedFile != null) {
                // 创建成功
                if (this.mappedFiles.isEmpty()) {
                    // 设置第一个的标记
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 获取当前正在写的顺序写的 MappedFile 对象，存储消息 或者 存储 ConsumerQueue 数据的时候 都需要获取当前的 MappedFile 对象
     * 注意：如果 MappedFile 写满了 或者不存在，则创建新的 MappedFile
     *
     * @return 当前正在写的顺序写的 MappedFile 对象
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                // 最后一个对象
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    /**
     * TODO 这个方法干嘛呢
     *
     * @param offset
     * @return
     */
    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {

            // 最后一个文件的写入位点的物理偏移量
            long lastOffset = mappedFileLast.getFileFromOffset() + mappedFileLast.getWrotePosition()/*最后一个文件的写入位点*/;

            // TODO ????
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff) {
                return false;
            }
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()/*从后往前遍历*/) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    /**
     * 获取 MappedFileQueue 管理的最小物理偏移量，其实就是获取 list(0) 这个文件名称表示的偏移量
     */
    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                MappedFile mappedFile = this.mappedFiles.get(0);
                return mappedFile.getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    /**
     * 获取 MappedFileQueue 管理的最大物理偏移量，当前顺序写的 MappedFile 文件名 + MappedFile.wrotePos
     */
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition()/*wrotePositon*/;
        }
        return 0;
    }

    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    /**
     * 该方法为 CommitLog 删除过期文件使用，根据文件保留时长决定释放删除文件
     *
     * @param expiredTime 过期时间
     * @param deleteFilesInterval 删除2个文件的时间间隔
     * @param intervalForcibly mf.destroy(intervalForcibly)
     * @param cleanImmediately true:强制删除不考虑过期时间这个条件
     * @return 删除的数量
     */
    @SuppressWarnings("all")
    public int deleteExpiredFileByTime(final long expiredTime, final int deleteFilesInterval, final long intervalForcibly, final boolean cleanImmediately) {

        // 复制
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs) {
            return 0;
        }

        // 当前数组长度 - 1，因为当前正在顺序写的文件肯定是不能删除的
        int mfsLength = mfs.length - 1;
        // 删除文件数量
        int deleteCount = 0;
        // 被删除的文件列表
        List<MappedFile> files = new ArrayList<>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];

                // 上次修改时间 + 过期时间 = 当前文件存活时间截止点
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;

                if (System.currentTimeMillis() >= liveMaxTimestamp/*文件存活时间达到上限*/ || cleanImmediately /*目录 disk 占用率达到上限的时候会设置该参数为 true ，强制删除*/) {
                    // 如果当前文件已经很久没修改了，或者要求立即删除，则进入该分支
                    if (mappedFile.destroy(intervalForcibly)) {
                        // 成功，加入删除列表
                        files.add(mappedFile);
                        deleteCount++;
                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            // 如果超过了每次删除的最大数量，则停止了
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                // 间隔一段时间
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        // 将删除文件的 mf 从 queue 中移除
        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 该方法为 ConsumerQueue 删除过期文件时候， offset 一般是指 CommitLog 内第一条消息的 offset，
     * 遍历每个 mappedFile对象，读取mappedFile最后一条数据，提取出 CAData > msgPhyOffset值，如果这个值 < offset,则删除该mappedFile文件
     *
     * @param offset 偏移量（commitLog 目录下的最小的物理偏移量）
     * @param unitSize 每个数据单元的固定大小
     * @return 删除数量
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            // 当前数组长度 - 1，因为当前正在顺序写的文件肯定是不能删除的
            int mfsLength = mfs.length - 1;

            // 遍历当前正在顺序写的前面的文件
            for (int i = 0; i < mfsLength; i++) {
                //是否需要删除
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];

                // 获取当前文件最后一个数据单元
                int pos = this.mappedFileSize - unitSize;
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
                if (result != null) {

                    // 读取最后一个数据单元的前八个字节(cqData)，其实就是消息的偏移量
                    ByteBuffer byteBuffer = result.getByteBuffer();
                    long maxOffsetInLogicQueue = byteBuffer.getLong();

                    // 释放
                    result.release();

                    // 如果true则说明：当前mf内所有的cqData都是过期数据
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset " + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        // 从队列中删除
        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 根据 flushLeastPages 查找合适的 MappedFile 对象，调用该 MappedFile 的落盘方法，并且更新全局 flushedWhere 值
     *
     * @param flushLeastPages 0：强制刷盘，>0 脏页数据必须达到该值的时候才刷盘
     * @return true：本次刷盘无数据落盘，false:本次刷盘有数据落盘
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;

        // 获取当前正在刷盘的文件，大概率就是当前正在顺序写的文件
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {

            // 获取最后一条消息的存储时间
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            // 刷盘，返回mf最新的落盘位点
            int offset = mappedFile.flush(flushLeastPages);
            // mf最新的落盘位点 + mf其实偏移量
            long where = mappedFile.getFileFromOffset() + offset;

            // 如果 where == this.flushedWhere 则说明根本就没刷盘数据
            result = where == this.flushedWhere;
            // 赋值执行刷盘位点
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                // 强制刷盘保存时间
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere;
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * Finds a mapped file by offset.
     *
     * 根据偏移量查找区间包含该 offset 的 MappedFile 对象
     *
     * @param offset Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            // 第一个
            MappedFile firstMappedFile = this.getFirstMappedFile();
            // 最后一个
            MappedFile lastMappedFile = this.getLastMappedFile();

            if (firstMappedFile != null && lastMappedFile != null) {
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    // 传入的 偏移量 是否能够通过校验，进来说明不通过，瞎几把传的，打印日志
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}", offset, firstMappedFile.getFileFromOffset(),
                            lastMappedFile.getFileFromOffset() + this.mappedFileSize, this.mappedFileSize, this.mappedFiles.size());
                } else {
                    // 传入的 偏移量 通过校验

                    // 滚轮删除，老的首文件可能已经被删除了，所以新的首文件的 fileFromOffset 可能已经不是0了，所以需要计算下标'
                    // 比如说： commitLog目录下有文件：5g,6g,7g,8g,9g,10g,我们要查找 offset 为 7.6g 的mappedFile
                    // index = (7.6/1) - (5/1)
                    // index = 2.6
                    // (int)2.6 = 2
                    // 所以 index = 2
                    // targetFile = mappedFiles.get(2) 其实就是 7g 这个文件
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    if (targetFile != null && offset >= targetFile.getFileFromOffset() && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        // 再次校验下偏移量，一般情况下满足
                        // 正常情况在此返回了
                        return targetFile;
                    }

                    // 遍历全部的 mappedFile ，根据偏移量查询
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset() && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    /**
     * 查询包含该 offset 的 mappedFile
     *
     * @param offset
     * @return
     */
    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }
        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }
}

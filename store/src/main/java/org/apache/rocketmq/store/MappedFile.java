package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 0.....落盘数据.....flushedPosition.....脏页数据.....wrotePosition.......空闲部分.....文件结尾
 *
 * commitLog 顺序写的文件
 * consumerQueue
 * indexFile
 *
 * 系统调用：
 * mmap 零拷贝
 */
@SuppressWarnings("all")
public class MappedFile extends ReferenceResource {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 操作系统内存页大小 4K， PAGE_CACHE
    public static final int OS_PAGE_SIZE = 1024 * 4;

    /**
     * 当前进程下，所有的 MappedFile 占用的总的虚拟内存的大小
     *
     * @see MappedFile#init(java.lang.String, int) 每次初始化一个mappedFile对象之后都会增加，TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
     */
    private static/*当前进程唯一！*/ final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 当前进程下，所有的 MappedFile 对象的个数
     *
     * @see MappedFile#init(java.lang.String, int)
     */
    private static/*当前进程唯一！*/ final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     * 跟 wrotePosition 一样的作用，先不考虑这个
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    /**
     * @see MappedFile#wrotePosition
     * @see MappedFile#committedPosition 跟 {@link MappedFile#wrotePosition} 相同的功能，什么时候用这个字段呢？只有当前文件不使用{@link MappedFile#mappedByteBuffer} 的时候才用 committedPosition，而是用 {@link MappedFile#writeBuffer}的时候
     *
     * 从开始到 flushedPosition 这段数据是保证落盘的，从 flushedPosition 到 wrotePosition 之间的这部分数据是可能未落盘的！称为脏页数据，从 wrotePosition 到 最后 是空闲部分
     *
     * 0.....落盘数据.....flushedPosition.....脏页数据.....wrotePosition.......空闲部分.....文件结尾
     * @see MappedFile#flushedPosition
     *
     * 当前数据写入到 MappedFile 的位点，写入位点
     * @see MappedFile#appendMessagesInner(org.apache.rocketmq.common.message.MessageExt, org.apache.rocketmq.store.AppendMessageCallback)
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    // 0 ------- 落盘数据(安全数据) --------- flushedPosition ------- 脏页(不安全的数据) ------- wrotePosition -------- 空闲 -------.....

    /**
     * 刷盘位点
     *
     * 0 ------- 落盘数据(安全数据) --------- flushedPos ------- 脏页(不安全的数据) ------- wrotePos -------- 空闲 -------.....
     *
     * 从开始到 flushedPosition 这段数据是保证落盘的，从 flushedPosition 到 wrotePosition 之间的这部分数据是未落盘的！称为脏页数据，从 wrotePosition 到 最后 是空闲部分
     *
     * @see MappedFile#flush(int)
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    // 文件大小
    @Getter
    protected int fileSize;

    // 文件访问通道

    /**
     * RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw");
     * this.fileChannel = randomAccessFile.getChannel();
     *
     * @see MappedFile#file 该文件的通道
     */
    @Getter
    protected FileChannel fileChannel;

    /**
     * 瞬态存储池
     * 内存池！！！！一般不用
     */
    protected TransientStorePool transientStorePool = null;

    /**
     * 绝对路径文件名
     * <P></P>
     * commitLog:文件名就是该文件存储第一条消息的物理偏移量，如：/Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/broker/store/commitlog/00000000000000000000
     * <P></P>
     * consumerQueue：文件名也是第一条消息的偏移量，
     * <P></P>
     * indexFile:文件名就是 年月日时分秒
     */
    @Getter
    private String fileName;

    // 文件名称转long 偏移量

    /**
     * this.fileFromOffset = Long.parseLong(this.file.getName()); // 通过文件名转 long 得到！
     * 如：00000000000000000000
     */
    @Getter
    private long fileFromOffset;

    // 文件

    /**
     * this.file = new File(fileName); // 创建文件
     */
    @Getter
    private File file;

    // 内存映射缓冲区，访问虚拟内存

    /**
     * 不用看
     *
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     *
     * @see MappedFile#init(java.lang.String, int, org.apache.rocketmq.store.TransientStorePool)
     *
     * 内存池的回城，一般不用，跟{@link MappedFile#transientStorePool} 配套使用
     */
    protected ByteBuffer writeBuffer = null;

    /**
     * 该对象{@link MappedFile#mappedByteBuffer}会在当前进程的虚拟空间内开辟一片内存空间，大小跟文件大小{@link MappedFile#file} 是 一比一的，该虚拟空间由该对象控制！！！！通过该对象{@link MappedFile#mappedByteBuffer}可以对文件进行任意位置的读写
     *
     * 虚拟内存不一定真正的在物理内存上开辟来空间，而是在你访问的时候去开辟（通过缺页异常）
     *
     *
     * 总结
     * MappedByteBuffer使用虚拟内存，因此分配(map)的内存大小不受JVM的-Xmx参数限制，但是也是有大小限制的。
     * 如果当文件超出1.5G限制时，可以通过position参数重新map文件后面的内容。
     * MappedByteBuffer在处理大文件时的确性能很高，但也存在一些问题，如内存占用、文件关闭不确定，被其打开的文件只有在垃圾回收的才会被关闭，而且这个时间点是不确定的。
     *
     * @see MappedFile#fileChannel
     */
    //this.mappedByteBuffer = this.fileChannel.map/*FileChannel提供了map方法把文件映射到虚拟内存*/(MapMode.READ_WRITE, 0/*文件映射时的起始位置。*/, fileSize);
    @Getter
    private MappedByteBuffer mappedByteBuffer;

    /**
     * 该文件最近 msg 的存储时间
     *
     * this.storeTimestamp = result.getStoreTimestamp();
     *
     * @see MappedFile#appendMessagesInner(org.apache.rocketmq.common.message.MessageExt, org.apache.rocketmq.store.AppendMessageCallback) 每次追加消息之后都会重新赋值
     */
    @Getter
    private volatile long storeTimestamp = 0;

    /**
     * 当前对象在组内是否是第一个文件，则为true
     * 按文件名称排序
     * 00000000000000000000 则该文件的 firstCreateInQueue 为 true，后面的都是false
     * 00000000000000012314
     * 00000000000000032134
     */
    @Getter
    @Setter
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    /**
     * @param fileName 绝对路径文件名
     * @param fileSize 文件大小
     */
    public MappedFile(final String fileName/*绝对路径文件名*/, final int fileSize/*文件大小*/) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    /**
     * 确定目录没有问题，不存在则创建
     */
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    // 当前进程下，所有的 MappedFile 对象的个数
    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    // 当前进程下，所有的 MappedFile 占用的总的虚拟内存的大小
    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     * @see AllocateMappedFileService#mmapOperation()
     */
    public void init(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName); // 创建文件
        this.fileFromOffset = Long.parseLong(this.file.getName()); // 通过文件名称赋值
        boolean ok = false;

        // 确定目录没有问题，不存在则创建
        ensureDirOK(this.file.getParent());

        try {
            // 创建文件通道，NIO 的东西
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw");
            this.fileChannel = randomAccessFile.getChannel();

            /*
             * TODO
             * 这个比较重要！！！！！
             * 获取内存映射缓冲区，用于访问虚拟内存
             *
             * FileChannel提供了map方法把文件映射到虚拟内存
             */
            this.mappedByteBuffer = this.fileChannel.map/*FileChannel提供了map方法把文件映射到虚拟内存*/(MapMode.READ_WRITE/*可读可写*/, 0/*文件映射时的起始位置。*/, fileSize);

            // 文件对象
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);

            // 对象个数+1
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    /**
     * @param msg 消息，服务端包装之后的
     * @param cb 追加消息回调，控制消息中的哪些字段需要追加，哪些不需要追加，都由该对象控制 {@link CommitLog.DefaultAppendMessageCallback}
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    /**
     * @param messageExt 消息，服务端包装之后的
     * @param cb 追加消息回调，控制消息中的哪些字段需要追加，哪些不需要追加，都由该对象控制  {@link CommitLog.DefaultAppendMessageCallback}
     * @see CommitLog.DefaultAppendMessageCallback 实现
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;
        // 当前内存映射的写入位点
        int currentPos = this.wrotePosition.get();
        if (currentPos/*wrotePosition*/ < this.fileSize) { // 条件成立：说明文件还没有满，可以继续写入
            // 使用内存映射创建切片
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice()/*一般用这个，他的pos跟limit没动*/;
            // 写入切片，不影响之前的
            byteBuffer.position(currentPos/*wrotePosition，设置切片的写入位点为 currentPos，则下次写入内容的时候从 currentPos + 1 开始*/);
            AppendMessageResult result;
            if (messageExt instanceof MessageExtBrokerInner) {
                // 向内存映射追加数据，具体由该回调对象控制
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos /*剩余空间*/, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                // 向内存映射追加数据，具体由该回调对象控制
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }

            // 本次总共追加（写入）的总字节数量
            int wroteBytes = result.getWroteBytes();
            // 更新数据写入的位点
            this.wrotePosition.addAndGet(wroteBytes);
            // 保存最后一条消息的存储时间
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }

        //
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    /**
     * 顺序追加数据
     *
     * @param data 需要写入到文件的字节
     */
    public boolean appendMessage(final byte[] data) {
        int currentPos/*当前内存映射的写入位点*/ = this.wrotePosition.get();
        if ((currentPos + data.length) <= this.fileSize) { // 条件成立：说明文件还没有满，可以继续写入
            try {
                // 设置写入位点
                this.fileChannel.position(currentPos);
                ByteBuffer byteBuffer/*把data包装起来*/ = ByteBuffer.wrap(data);
                this.fileChannel.write/*通过文件通道写入*/(byteBuffer);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            // 更新数据写入的位点
            this.wrotePosition.addAndGet(data.length);
            return true;
        }
        // 文件剩余空间不够了，无法追加
        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * data 中的部分数据写入当前对象对应的文件中
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos/*当前内存映射的写入位点*/ = this.wrotePosition.get();
        if ((currentPos + length) <= this.fileSize) {// 条件成立：说明文件还没有满，可以继续写入
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            // 更新数据写入的位点
            this.wrotePosition.addAndGet(length);
            return true;
        }
        // 文件剩余空间不够了，无法追加
        return false;
    }

    /**
     * @param flushLeastPages 刷盘的最小页数，当为0的时候则属于强制刷盘，大于0的时候需要脏页数据达到 传入的值的时候才进行物理刷盘
     * @return The current flushed position 当前刷盘点
     */
    public int flush(final int flushLeastPages/*刷盘的最小页数，当为0的时候则属于强制刷盘，大于0的时候需要脏页数据页数 达到 传入的值的时候才进行物理刷盘*/) {
        // 判断是否可以刷盘
        boolean ableToFlush = this.isAbleToFlush(flushLeastPages);
        if (ableToFlush) {
            if (this.hold()/* 引用计数 +1，保证刷盘过程中不会释放资源！！！！！！！*/) {
                int value/* 获取数据写入位点 wrotePosition*/ = getReadPosition();
                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        // 落盘！！！！！把数据刷盘到文件
                        this.fileChannel.force(false);
                    } else {
                        // 强制把数据刷到文件
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }
                /**
                 * 因为 mappedByteBuffer.force(); 方法保证把所以脏页数据都刷盘到磁盘，所以，刷盘完成之后数据写入位点跟刷盘位点要对齐！！！
                 * 写入位点赋值给 刷盘点,刷盘点跟写入点平齐了
                 */
                this.flushedPosition.set(value/* 获取数据写入位点 wrotePosition*/);
                // 刷盘完成之后就引用计数-1
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        // 返回最小的刷盘点
        return this.getFlushedPosition()/*flushedPosition 落盘位点，可能刷盘了，也可能没刷盘*/;
    }

    /**
     * 判断是否可以刷盘
     *
     * @param flushLeastPages 刷盘的最小页数，当为0的时候则属于强制刷盘，大于0的时候需要脏页数据达到 传入的值的时候才进行物理刷盘
     * @return 是否可以刷盘
     */
    private boolean isAbleToFlush(final int flushLeastPages/*盘的最小页数，当为0的时候则属于强制刷盘，大于0的时候需要脏页数据达到 传入的值的时候才进行物理刷盘*/) {
        // 当前刷盘位点
        int flush = this.flushedPosition.get();
        int write/*wrotePosition 当前写入位点*/ = getReadPosition();
        if (this.isFull()) {
            // 当前文件已经满了，返回 true 告诉调用方 必须要刷盘了
            return true;
        }
        if (flushLeastPages > 0) {
            // 刷盘的最小页数，当为0的时候则属于强制刷盘，大于0的时候需要脏页数据达到 传入的值的时候才进行物理刷盘
            // 如果 脏页 >= flushLeastPages 则刷盘

            int dirtyPages/*脏页数*/ = (write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE);

            return dirtyPages >= flushLeastPages;
        }
        // flushLeastPages <= 0 的情况
        // 执行到这里，说明传入的 flushLeastPages <= 0 的。则只要由脏数据就刷盘
        return write > flush/*如果写入位点在落盘位点的前面，如0....flush.....write.....limit,此时肯定有脏页数据了*/;
    }

    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }

        boolean ableToCommit = this.isAbleToCommit(commitLeastPages);
        if (ableToCommit) {
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();
        if (this.isFull()) {
            return true;
        }
        if (commitLeastPages > 0) {
            int dirtyPages/*脏页数*/ = (write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE);
            return dirtyPages >= commitLeastPages;
        }
        // flushLeastPages <= 0 的情况
        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        // 写满了
        return this.fileSize == this.wrotePosition.get();
    }

    /**
     * 从该文件中获取部分缓冲区，如：
     * 文件缓冲区：0.............wrotePosition ......... fileSize
     * 返回缓冲区：0....pos......pos+size.....wrotePosition,返回的就是 pos 到 pos + size 的部分内容
     *
     * @param pos 开始
     * @param size 大小
     * @return 返回的就是 pos 到 pos + size 的部分内容
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition/*写入位点，wrotePosition*/ = getReadPosition();
        if ((pos + size) <= readPosition/*wrotePosition*/) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos/*从 pos 开始*/);
                ByteBuffer byteBufferNew/*从 pos 开始到文件结尾的缓冲区*/ = byteBuffer.slice();
                byteBufferNew.limit(size/* 限制只能访问 pos + size 个字节 */);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: " + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size + ", fileFromOffset: " + this.fileFromOffset);
        }
        return null;
    }

    /**
     * 该方法返回从 pos 为开始位点，到 当前文件的 有效数据为止，创建一个切片 byteBuffer 供业务程序访问数据
     *
     * 其实就是得到了 pos 到 wrotePos 这段字节数组到内容
     *
     * @param pos 返回切片数据起始位点
     * @return 结果 当前文件pos - wrotePos 之间的切片
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        // 获取写入位点，可以理解是可读的位点，就是在此位点前是可读的
        int readPosition/*写入位点，wrotePosition*/ = getReadPosition();
        if (pos < readPosition && pos >= 0) { // pos 是有效数据访问内的位点
            // 条件成立：说明 pos 处于有效数据之前
            if (this.hold()/*引用数据+1，避免资源回收，外部要用了*/) {
                // 切片
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition/*写入位点*/ - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                /**
                 * 0...........wrotePos.......结尾
                 *
                 * 0... pos ...wrotePos.......结尾
                 *
                 * slice
                 *
                 * 0...position(pos).......
                 * size = wrotePos - pos 其实就是 pos 到 写入位点这段内容
                 * limit = size
                 *
                 * 其实就是得到了 pos 到 wrotePos 这段字节数组到内容
                 */
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    /**
     * 清理回收资源
     *
     * @param currentRef 当前对象被引用的次数，引用计数
     * @return 是否清理完成
     * @see ReferenceResource
     */
    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            // 还没有关闭呢
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            // 已经清理完成
            log.error("this file[REF:" + currentRef + "] " + this.fileName + " have cleanup, do not do it again.");
            return true;
        }

        // 执行清理逻辑，释放堆外内存
        MappedFileAssist.clean(this.mappedByteBuffer);
        // 当前进程下，所有的 MappedFile 占用的总的虚拟内存的大小
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        // 当前进程下，所有的 MappedFile 对象的个数
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    // 删掉当前文件使用该方法
    public boolean destroy(final long intervalForcibly) {
        // 先关闭
        this.shutdown(intervalForcibly);
        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");
                long beginTime = System.currentTimeMillis();
                // 删除
                boolean result = this.file.delete();
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }
            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName + " Failed. cleanupOver: " + this.cleanupOver);
        }
        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        /**
         * @see MappedFile#wrotePosition
         * @see MappedFile#committedPosition 跟 {@link MappedFile#wrotePosition} 相同的功能，什么时候用这个字段呢？只有当前文件不使用{@link MappedFile#mappedByteBuffer} 的时候才用 committedPosition，而是用 {@link MappedFile#writeBuffer}的时候
         *
         */
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 预热，会真实的在内存中开辟空间，避免发生缺页异常
     *
     * 内存映射文件预热，
     *
     * 既然空文件和写入1G字节虚拟内存映射都是1G大小，写入1G大小的意义呢？
     *
     * 使用mmap()内存分配时，只是建立了进程虚拟地址空间，并没有分配虚拟内存对应的物理内存。当进程访问这些没有建立映射关系的虚拟内存时，处理器自动触发一个缺页异常，进而进入内核空间分配物理内存、更新进程缓存表，最后返回用户空间，恢复进程运行。
     *
     * 小结：写入这些假值的意义在于实际分配物理内存，在消息写入时防止缺页异常
     *
     * @see <a href="https://yongliangcode.github.io/posts/708c22d7/">MQ25# RocketMQ存储--映射文件预热</a>
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j/*控制线程睡眠*/ = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE/*4KB*/, j++) {
            // 都写入 0
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    // 落盘
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    /*
                     * 每当写入1000个字节的额时候，执行
                     * 当前线程放弃CPU进入就绪状态，重新竞争CPU
                     * 防止一直独占CPU
                     */
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }
        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}", this.getFileName(), System.currentTimeMillis() - beginTime);
            // 同步刷盘时将未落盘的数据写入磁盘
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(), System.currentTimeMillis() - beginTime);
        /**
         * 内存锁定
         * 这样内存不会到swap区，会一直占用物理内存，这样的话写性能非常好
         */
        this.mlock();
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    /**
     * 内存锁定
     * 这样内存不会到swap区，会一直占用物理内存，这样的话写性能非常好
     */
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    /**
     * 解锁内存
     *
     * @see DefaultMessageStore#unlockMappedFile(org.apache.rocketmq.store.MappedFile)
     */
    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}

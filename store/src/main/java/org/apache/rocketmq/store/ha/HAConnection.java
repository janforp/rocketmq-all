package org.apache.rocketmq.store.ha;

import lombok.Getter;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

@SuppressWarnings("all")
public class HAConnection {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final HAService haService;

    /**
     * slave 节点 跟 当前 master 节点的连接
     */
    @Getter
    private final SocketChannel socketChannel;

    /**
     * this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
     */
    private final String clientAddr;

    // 写

    /**
     * master 节点 向 slave节点发送 commitLog 中的数据，进行主从复制
     */
    private final WriteSocketService writeSocketService;

    // 读

    /**
     * slave 向 master 上报的是 slave 本地的同步进度，这个同步进度就是一个 long 值，
     * 所以，该服务处理的桢格式为：
     * [long][long][long][long]。。。。
     */
    private final ReadSocketService readSocketService;

    /**
     * 在 slave 上报过 本地的 maxOffset 之后会被赋值。它 >= 0 之后同步数据的逻辑才会执行
     *
     * why?因为 master 不知道 跟他连接的 slave 节点当前的消息存储进度是在哪里了,它就没办法去给slave推送数据，因为不知道从哪个位点开始
     *
     * @see ReadSocketService#processReadEvent() 在该方法赋值
     */
    private volatile long slaveRequestOffset = -1;

    /**
     * 保存最新的 slave 上的 offset 信息， slaveAckOffset 之前的数据，都可以认为 slave 已经全部同步完成了
     * TODO 对应的 生产者线程 需要被唤醒 ！！！！！！
     *
     * @see ReadSocketService#processReadEvent() 在该方法赋值
     * @see HAService.HAClient#reportSlaveMaxOffset(long) slave 节点向 master 节点上报自己已经同步的位点
     */
    private volatile long slaveAckOffset = -1;

    /**
     * @see HAService.AcceptSocketService#run() 服务端接受到连接请求之后就会创建一个该对象
     */
    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);

        // 设置 socket 读写缓冲区 64 kb
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);

        // 创建读写服务
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);

        // 统计自增
        this.haService.getConnectionCount().incrementAndGet();
    }

    /**
     * @see HAService.AcceptSocketService 在该对象的 run 方法中调用这个方法
     */
    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    /**
     * slave 向 master 上报的是 slave 本地的同步进度，这个同步进度就是一个 long 值，
     * 所以，该服务处理的桢格式为：
     * [long][long][long][long]。。。。
     */
    class ReadSocketService extends ServiceThread {

        // 1 mb
        private static final int READ_MAX_BUFFER_SIZE = 1024/*m*/ * 1024/*k*/ * 1;

        private final Selector selector;

        /**
         * slave 节点 跟 当前 master 节点的连接
         */
        private final SocketChannel socketChannel;

        // 1mb 读缓冲区
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        /**
         * 缓冲区处理位点
         * 该缓冲块上次处理的位点，也就是说该位点之前的数据都是已经处理过的过期数据
         */
        private int processPosition = 0;

        // 上次读时间
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;

            // 将 master slave 会话 注册到多路复用器 监听 OP_READ 事件
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            while (!this.isStopped()) {
                try {
                    // 最长阻塞 1 s
                    this.selector.select(1000);

                    // 1.可以读数据了，事件就绪.2.1秒钟超时了
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        // 跳出循环
                        break;
                    }

                    // 长时间没有传输数据了
                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            // 停止了

            this.makeStop();

            // 将读服务对应的写服务也关闭
            writeSocketService.makeStop();

            // 移除连接
            haService.removeConnection(HAConnection.this);

            // 自减
            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            // 取消事件
            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                // 半关闭了，本地的事件也应该取消
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }

        /**
         * 处理读事件
         *
         * @return true 表示处理成功，false 表示 socket 关闭
         */
        private boolean processReadEvent() {

            // 循环控制变量，连续 读取失败3次跳出循环
            int readSizeZeroTimes = 0;

            if (!this.byteBufferRead.hasRemaining()) {
                // 在真正从 socket 读缓冲区读取数据到该缓存块之前该缓冲块已经满了

                // 说明 byteBufferRead 没有剩余空间了

                this.byteBufferRead.flip() /* 相当于清理操作， pos = 0  */;
                // 归零
                this.processPosition = 0;
            }

            // 执行到这里说明 byteBufferRead 缓冲块 还有空闲
            while (this.byteBufferRead.hasRemaining() /*有空闲空间*/) {
                try {
                    /**
                     * slave 向 master 上报的是 slave 本地的同步进度，这个同步进度就是一个 long 值，
                     * 所以，该服务处理的桢格式为：
                     * [long][long][long][long]。。。。
                     * @see HAService.HAClient#reportSlaveMaxOffset(long) slave 节点向 master 节点上报自己已经同步的位点
                     */
                    int readSize /*本次读取到的字节数*/ = this.socketChannel.read/*从 socket 中读取数据到缓冲区*/(this.byteBufferRead);
                    if (readSize > 0) {
                        // 重置次数
                        readSizeZeroTimes = 0;
                        // 记录最近一次读取到数据的时间
                        this.lastReadTimestamp/*当前时间戳*/ = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        boolean existOneFrameData =
                                (this.byteBufferRead.position()/*从socket缓冲区读取数据到该缓冲块的时候 position 会向 limit 方向移动*/ - this.processPosition/*该缓冲块上次处理的位点，也就是说该位点之前的数据都是已经处理过的过期数据*/) >= 8;/* 本次读取到的数据两大于 8 个字节，一个 long 的值肯定没问题 */
                        if (existOneFrameData /* 本次读取到的数据两大于 8 个字节，一个 long 的值肯定没问题 */) {

                            // pos 是啥呢？
                            // pos = 100 - 100 % 8 = 96
                            // pos = 107 - 107 % 8 = 104
                            // pos = 104 - 104 % 8 = 104
                            // pos 是 byteBufferRead 中可读数据 中的 最后一个（8字节） 桢的 数据，本次就是要从该 pos 位置开始读取数据 （前面的可以不要了因为 slave 端 上报的进度的间隔时间比较端，极有可能在较短时间内上报了多次，取最后一个即可）
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            // 读取最后一个桢的数据，就是读取 slave 端的同步进度
                            long readOffset/*slave节点上报的位点*/ = this.byteBufferRead.getLong(pos - 8);
                            // 更新处理位点
                            this.processPosition = pos;
                            HAConnection.this.slaveAckOffset = readOffset;
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }
                            // 唤醒阻塞的生产者线程（那些正在做存储消息的线程）
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            // 一般在此处跳出循环
                            break;
                        }
                    } else {
                        // readSize == -1，socket 半关闭状态,需要上层重建 HAConnection 对象
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * master 节点 向 slave节点发送 commitLog 中的数据，进行主从复制
     */
    class WriteSocketService extends ServiceThread {

        private final Selector selector;

        /**
         * slave 节点 跟 当前 master 节点的连接
         */
        private final SocketChannel socketChannel;

        // 协议头大小
        private final int headerSize = 8/*物理偏移量*/ + 4/*数据大小*/;

        // 桢头缓冲区
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize/*偏移量+大小 共 12 个字节*/);

        /**
         * 下次传输同步数据的位置信息，非常重要(master 需要知道给当前 slave 同步的位点)
         * 一般 this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
         */
        private long nextTransferFromWhere = -1;

        // 内部有 byteBuffer，封装了 commitLog 数据
        private SelectMappedBufferResult selectMappedBufferResult;

        // 上一轮数据是否传输完毕？
        private boolean lastWriteOver = true;

        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            // 注册事件
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE/*关注写数据事件*/);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    /**
                     * 在 slave 上报过 本地的 maxOffset 之后会被赋值。它 >= 0 之后同步数据的逻辑才会执行
                     *
                     * why?因为 master 不知道 slave 节点当前的消息存储进度是在哪里了,它就没办法去给slave推送数据
                     */
                    if (-1 == HAConnection.this.slaveRequestOffset/*slave节点上报的位点*/) {
                        // slave 节点还没上报自己的进度，master 节点就不知道从哪里开始发送数据，所以要等待
                        Thread.sleep(10);

                        // 继续循环，等待 slave 节点上报自己的同步位点
                        continue;
                    }
                    DefaultMessageStore defaultMessageStore = HAConnection.this.haService.getDefaultMessageStore();
                    if (-1 == this.nextTransferFromWhere/*初始化的时候就是 -1，说明还没有被重新赋值*/) {
                        if (0 == HAConnection.this.slaveRequestOffset/*初始化是-1，如果是0则说明上报了*/) {
                            // master 最大的 offset
                            long masterOffset = defaultMessageStore.getCommitLog().getMaxOffset();

                            // 计数 maxOffset 归属的 mappedFile 文件的 开始的 offset
                            int mappedFileSizeCommitLog = defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();

                            // TODO  多次用到这个算法！！！！！
                            masterOffset = masterOffset - (masterOffset % mappedFileSizeCommitLog/*1G*/);
                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            // 意思就是 如果 slaveRequestOffset 是 0 就是从最后一个正在顺序写的 mappedFile 开始同步数据
                            this.nextTransferFromWhere = masterOffset;
                        } else {

                            // 一般从这里赋值
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }
                    }

                    // 执行到这，说明位点数值已经没问题了

                    if (this.lastWriteOver /*上一轮的 桢 数据发送完成，默认是 true */) {
                        long interval/*上次写数据到目前为止的时间*/ = defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
                        if (interval/*上次写数据到目前为止的时间*/ > defaultMessageStore.getMessageStoreConfig().getHaSendHeartbeatInterval()/*5秒*/) {
                            // 如果超过了 5秒则写数据，否则继续循环

                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize/*12*/);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere/*偏移量*/);
                            this.byteBufferHeader.putInt(0/*因为本次发送的是心跳数据*/);
                            this.byteBufferHeader.flip();

                            // 发送一个 header 数据包，用于维持长连接
                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver) {
                                continue;
                            }
                        }
                    } else {
                        /*上一轮的 桢 数据 没有发送完成*/
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver) {
                            continue;
                        }
                    }

                    // 执行到这，说明本次数据发送成功了！！！！！！！

                    // 到 commitLog 中查询 nextTransferFromWhere 开始位置的数据
                    SelectMappedBufferResult selectResult = defaultMessageStore.getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        // size 可能很大，有可能是一整个文件
                        int size = selectResult.getSize();
                        if (size > defaultMessageStore.getMessageStoreConfig().getHaTransferBatchSize()/*32K*/) {

                            // 超过 32K 则设置 32K
                            size = defaultMessageStore.getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        // 增加size 大小，方便下一轮传输跳过本桢数据
                        this.nextTransferFromWhere = nextTransferFromWhere + size;

                        selectResult.getByteBuffer().limit(size/*设置该缓冲区可访问区间，本次发送的字节数,最多32K*/);
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset/* 本桢数据的开始偏移量 */);
                        this.byteBufferHeader.putInt(size /* 本桢数据大小 */);
                        this.byteBufferHeader.flip();

                        // true：不是处理完成
                        this.lastWriteOver = this.transferData();
                    } else {
                        // 等待
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            // 跳出了 while 循环，this.isStopped() == true

            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();
            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }
            this.makeStop();
            readSocketService.makeStop();
            haService.removeConnection(HAConnection.this);
            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }
            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
            HAConnection.log.info(this.getServiceName() + " service end");
        }

        /**
         * 先写头，再写body
         *
         * @return
         * @throws Exception
         */
        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp/*当前时间*/ = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {

                        // 读取循环一般从这里跳出
                        break;
                    }
                } else {
                    // writeSize == -1 说明连接已经半关闭
                    throw new Exception("ha master write header error < 0");
                }
            }

            // 当前桢数据发送成功之后，设置该字段为null
            if (null == this.selectMappedBufferResult /* 心跳包 */) {
                return !this.byteBufferHeader.hasRemaining() /* 心跳是否发送完成 */;
            }

            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining() /* 只有 header 全部写成功之后才开始写 body */) {
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining() /* body 缓冲区中有待处理数据 */) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        // 写缓冲区满了
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        // 连接关闭了
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            // 二者都没数据了则返回 true
            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining() /* 表示 selectMappedBufferResult 数据全部写完成了 */) {
                // help gc
                this.selectMappedBufferResult.release();
                // 当前桢数据发送成功之后，设置该字段为null
                this.selectMappedBufferResult = null;
            }

            return result;
        }
    }
}
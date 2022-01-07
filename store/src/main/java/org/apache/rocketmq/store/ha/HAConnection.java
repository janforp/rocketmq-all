package org.apache.rocketmq.store.ha;

import lombok.Getter;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class HAConnection {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final HAService haService;

    @Getter
    private final SocketChannel socketChannel;

    private final String clientAddr;

    // 写
    private final WriteSocketService writeSocketService;

    // 读
    private final ReadSocketService readSocketService;

    /**
     * 在 slave 上报过 本地的 maxOffset 之后会被赋值。它 >= 0 之后同步数据的逻辑才会执行
     *
     * why?因为 master 它不知道 slave 节点当前的消息存储进度是在哪里了？它就没办法去给slave推送数据
     */
    private volatile long slaveRequestOffset = -1;

    /**
     * 保存最新的 slave 上的 offset 信息， slaveAckOffset 之前的数据，都可以认为   slave 已经全部同步完成了
     * 对应的 生产者线程 需要被唤醒
     */
    private volatile long slaveAckOffset = -1;

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
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);

        // 统计自增
        this.haService.getConnectionCount().incrementAndGet();
    }

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
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;

        private final Selector selector;

        // master slave 会话
        private final SocketChannel socketChannel;

        // 1mb 缓冲区
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        // 缓冲区处理位点
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
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 最长阻塞 1 s
                    this.selector.select(1000);

                    // 1.可以读数据了，事件就绪.2.超时了
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }
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

            this.makeStop();

            writeSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            HAConnection.this.haService.getConnectionCount().decrementAndGet();

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

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
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

                // 说明 byteBufferRead 没有剩余空间了

                this.byteBufferRead.flip() /* 相当于清理操作， pos = 0  */;
                // 归零
                this.processPosition = 0;
            }

            while (this.byteBufferRead.hasRemaining() /*有空闲空间*/) {
                try {

                    /*
                     * slave 向 master 上报的是 slave 本地的同步进度，这个同步进度就是一个 long 值，
                     * 所以，该服务处理的桢格式为：
                     * [long][long][long][long]。。。。
                     */

                    // 从 socket 中读取数据到缓冲区
                    int readSize /*本次读取到的字节数*/ = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {

                        // 重置次数
                        readSizeZeroTimes = 0;

                        // 记录最近一次读取到数据的时间
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();

                        if ((this.byteBufferRead.position() - this.processPosition) >= 8 /* 本次读取到的数据两大于 8 个字节，一个 long 的值肯定没问题 */) {

                            // pos 是啥呢？
                            // pos = 100 - 100 % 8 = 96
                            // pos = 107 - 107 % 8 = 104
                            // pos = 104 - 104 % 8 = 104
                            // pos 是 byteBufferRead 中可读数据 中的 最后一个 桢的 数据，本次就是要从该 pos 位置开始读取数据 （前面的可以不要了）
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            // 读取最后一个桢的数据，就是读取 slave 端的同步进度
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            // 更新处理位点
                            this.processPosition = pos;

                            HAConnection.this.slaveAckOffset = readOffset;
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            // 唤醒阻塞的生产者线程（哪些正在做存储消息的线程）
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            // 一般在此处跳出循环
                            break;
                        }
                    } else {
                        // socket 半关闭状态
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

    class WriteSocketService extends ServiceThread {

        private final Selector selector;

        private final SocketChannel socketChannel;

        // 协议头大小
        private final int headerSize = 8 + 4;

        // 桢头缓冲区
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);

        // 下次传输同步数据的位置信息，非常重要(master 需要知道给当前 slave 同步的位点)
        private long nextTransferFromWhere = -1;

        // 内部有 byteBuffer
        private SelectMappedBufferResult selectMappedBufferResult;

        // 上一轮数据是否传输完毕？
        private boolean lastWriteOver = true;

        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            // 注册事件
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }
                    if (-1 == this.nextTransferFromWhere) {
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            // master 最大的 offset
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();

                            // 计数 maxOffset 归属的 mappedFile 文件的 开始的 offset
                            masterOffset = masterOffset - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getMappedFileSizeCommitLog());
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

                    if (this.lastWriteOver /*上一轮的 桢 数据发送完成*/) {
                        long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;
                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaSendHeartbeatInterval()) {
                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
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

                    // 到 commitLog 中查询 nextTransferFromWhere 开始位置的数据
                    SelectMappedBufferResult selectResult = HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {

                        // size 可能很大
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {

                            // 超过 32 则设置 32
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        // 增加size 大小，方便下一轮传输跳过本桢数据
                        this.nextTransferFromWhere += size;

                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        // true：不是处理完成
                        this.lastWriteOver = this.transferData();
                    } else {
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }
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

        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            if (null == this.selectMappedBufferResult) {
                // 可能是心跳包而已，并不是发送数据
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining() /*有待处理数据*/) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            // 二者都没数据了则返回 true
            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                // help gc
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
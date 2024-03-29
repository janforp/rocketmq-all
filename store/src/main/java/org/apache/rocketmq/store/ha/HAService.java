package org.apache.rocketmq.store.ha;

import lombok.Getter;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HA == high avaliable
 */
@SuppressWarnings("all")
public class HAService {

    /**
     * 表示当前主节点 有多少个 slave 节点 与其进行数据同步
     *
     * @see HAConnection#HAConnection(org.apache.rocketmq.store.ha.HAService, java.nio.channels.SocketChannel) 构造成功只就会自增
     */
    @Getter
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    // master 会给每个向其发起连接的 slave 节点(socketChannel) 创建一个 HAConnection 对象，它封装了 socketChannel ，控制 master 端向 slave 端 传输数据的逻辑
    private final List<HAConnection> connectionList = new LinkedList<>();

    // master 启动之后会绑定服务器指定端口，监听 slave 的连接， 该对象封装了这个逻辑
    // HA 这块并没有和 netty 那套逻辑混淆在一起，而是使用原生态的 NIO 去做的
    private final AcceptSocketService acceptSocketService;

    @Getter
    private final DefaultMessageStore defaultMessageStore;

    // 线程通信对象
    @Getter
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();

    // master 向 slave 推送的最大的 offset，可以理解为数据同步的进度
    @Getter
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    // 跟前面学习的 GroupCommitService 没太大区别，注意也是 控制 生产者线程 阻塞等待的理解
    private final GroupTransferService groupTransferService;

    // slave 节点的客户端对象， slave 端才会正常运行该实例
    // TODO 一般从这个对象开始分析 HA 的逻辑
    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService = new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    /**
     * 该方法是给 slave 节点调用的，master节点是不会调用该方法的！
     *
     * @param newAddr 地址
     */
    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    /**
     * @see CommitLog#handleHA 消息成功追加到 commitLog 之后就会调用该方法
     */
    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result = result && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore.getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * 通知线程同步一些数据给slave节点
     *
     * @param slave 节点已经同步的位点
     * @see HAService.HAClient#reportSlaveMaxOffset(long) slave 节点向 master 节点上报自己已经同步的位点
     */
    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public void start() throws Exception {
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {

        // master 服务器监听绑定的端口 10912
        private final SocketAddress socketAddressListen;

        // 服务器端的通道
        private ServerSocketChannel serverSocketChannel;

        // 多路复用器
        private Selector selector;

        /**
         * @param port 10912
         * @see HAService#HAService(org.apache.rocketmq.store.DefaultMessageStore)
         */
        public AcceptSocketService(final int port /* 10912 */) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);

            // 绑定
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            this.serverSocketChannel.configureBlocking(false);

            // 把通道注册到多路复用器并且监听 OP_ACCEPT 事件（就是客户端发起连接的事件）
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                ////log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            while (!this.isStopped()) {
                try {
                    // 在多路复用器阻塞最多1s
                    this.selector.select(1000);

                    // 1.OP_ACCEPT 事件就绪，2.超时了
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {

                        // 遍历事件
                        for (SelectionKey k : selected) {

                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0 /*当前事件是 客户端发起连接事件*/) {

                                // 拿到客户端的连接
                                ServerSocketChannel serverSocketChannel = (ServerSocketChannel) k.channel();
                                // 与客户端的连接
                                SocketChannel sc = (serverSocketChannel).accept();
                                if (sc != null) {
                                    try {
                                        // 给每个 slave 发起的连接对象封装到 一个 HAConnection 对象中去
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        // 前端 HAConnection 对象，包括读数据，写数据服务
                                        conn.start();
                                        // 塞入集合 this.connectionList.add(conn);
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        sc.close();
                                    }
                                }
                            } else {
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    ////log.error(this.getServiceName() + " service has exception.", e);
                }
            }
        }
    }

    /**
     * GroupTransferService Service
     *
     * 主从复制服务
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();

        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();

        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        long waitUntilWhen = HAService.this.defaultMessageStore.getSystemClock().now() + HAService.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();
                        while (!transferOK && HAService.this.defaultMessageStore.getSystemClock().now() < waitUntilWhen) {
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            ////log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        req.wakeupCustomer(transferOK);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                }
            }
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }
    }

    /**
     * slave 节点的客户端对象， slave 端才会正常运行该实例
     *
     * slave 端运行的 HA 客户端代码，他会和 master 服务器建立长连接，上报本地同步进度，消费服务器发来的 msg 数据...
     */
    class HAClient extends ServiceThread {

        // 4 mb
        private static final int READ_MAX_BUFFER_SIZE = 1024/*k*/ * 1024/*m*/ * 4;

        /**
         * ip:port 表示 master 节点启动时监听的 HA 会话端口（和 netty 绑定的服务端口不是同一个）
         * 什么时候赋值的呢？slave节点会赋值，master节点不会赋值
         *
         * @see DefaultMessageStore#updateHaMasterAddress(java.lang.String)
         * @see org.apache.rocketmq.broker.BrokerController#doRegisterBrokerAll(boolean, boolean, org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper)
         */
        private final AtomicReference<String> masterAddress = new AtomicReference<>();

        /**
         * 8 个字节，上报 offset 的时候使用，因为底层通信使用的 NIO 所有内容都是通过块传输的，所以上报 slave offset 的时候需要使用该 buffer
         * 刚好一个long类型的长度！！
         *
         * @see HAClient#reportSlaveMaxOffset(long)
         */
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);

        /**
         * 客户端与master的会话通道
         *
         * @see HAClient#connectMaster()
         */
        private SocketChannel socketChannel;

        // 多路复用器
        private Selector selector;

        /**
         * 上次会话通信时间，用于控制连接（socketChannel） 是否关闭，如果长时间连接空闲，则可能要关闭该连接
         *
         * @see HAClient#reportSlaveMaxOffset(long) 上报进度到master成功之后会更新该字段
         */
        private long lastWriteTimestamp = System.currentTimeMillis();

        /**
         * 首次跟master连接的时候就会更新该字段：this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
         *
         * this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
         *
         * slave 当前的进度信息，每次上报之后都会更新该字段，上报的时候会把该值写入到{@link HAClient#reportOffset},然后
         * {@link HAClient#reportOffset} 把进度写入到 {@link HAClient#socketChannel} ,最终写入到服务端
         */
        private long currentReportedOffset = 0;

        // TODO 控制byteBuffer position 指针使用的
        // 0 ... ................. ............ position ..................limit
        // 0 ... dispatchPosition ............. position ..................limit
        // 处于 dispatchPosition 到 position 之间的字节是还没有处理的字节
        // 每处理一桢数据之后，让 dispatchPosition 前进一桢的长度，则 dispatchPosition 之前的数据就是已经处理过的数据
        private int dispatchPosition = 0;

        /**
         * 了解下面2个字节数组的前提：
         *
         * master 与 slave 之间传输的数据格式：
         *
         * {[phyOffset1  固定8字节][size1 同步数据块的大小][data1 数据块，最大32kb，可能包含多条消息的数据，最后一条消息也有可能是不完整的]}{[phyOffset2][size2][data2]}{[phyOffset3][size3][data3]}
         * phyOffset:数据区间的开始偏移量，并不表示一条具体的消息，表示的数据块开始的偏移量
         * size:同步数据块的大小
         * data:数据块，最大32kb，可能包含多条消息的数据
         *
         * @see HAClient#processReadEvent()
         */

        // 用于到 socket 读缓冲区 加载 就绪的数据使用，4mb
        // byteBufferRead 加载完之后，做什么事情？基于 桢协议 去解析，解析出来的桢 然后存储到 slave 的 commitLog 内。
        // 处理数据的过程中，程序并没有去调整 byteBufferRead 的 position （调整过，但是解析完一条数据之后又给恢复原来的position） 指针。
        // 总之 byteBufferRead 会遇到 pos == limit 的时候，这种情况下，最后一条 桢 数据大概率是半包数据，程序总不能将它给丢掉，就将它 拷贝到 byteBufferBackup 这个缓冲区，然后
        // 将 byteBufferRead clean（其实就是将 pos 设置为 0），swap 交换  byteBufferBackup 成为 byteBufferRead，后者成为前者
        // 然后再使用 byteBufferRead(包含了半包数据) 到 socket 读缓冲区 加载剩余数据，然后程序就可能正常处理了
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE /* 4mb */);

        // 当上面的 byteBufferRead 最后一个包是半包的时候使用，会跟 byteBufferRead 进行一个 swap
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE /* 4mb */);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        /**
         * 如果当前节点是主节点，则这个方法肯定是不会被调用的，只有slave节点才会调用该方法
         *
         * @param newAddr
         */
        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                //log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        private boolean isTimeToReportOffset() {
            long interval/*上次上报的时间跟当前时间的差*/ = HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            // 每五秒 会主动上报一次 slave 端的同步进度给 master
            return interval/*上次上报的时间跟当前时间的差*/ > HAService.this.defaultMessageStore.getMessageStoreConfig().getHaSendHeartbeatInterval()/*5秒*/;
        }

        /**
         * 上报 slave 同步进度到 master
         *
         * @param maxOffset slave端的offset
         * @return 成功失败
         */
        private boolean reportSlaveMaxOffset(final long maxOffset/*当前slave节点的commitLog文件的最大偏移量*/) {
            // 8 个字节，上报 offset 的时候使用，因为底层通信使用的 NIO 所有内容都是通过块传输的，所以上报 slave offset 的时候需要使用该 buffer
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            // 传输的数据塞进去
            this.reportOffset.putLong(maxOffset);
            // 复位 8 -> 0
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 /*  尝试写3次，大概率一次性写成功，8个字节比较小 */ && this.reportOffset.hasRemaining() /* 写成功之后该条件就不会满足，跳出循环了 */; i++) {
                try {

                    // 通过 socket 把进度写到 master 节点
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    return false;
                }
            }

            // 更新同步时间
            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();

            // 写成功 之后 pos == limit
            boolean writeSuccess/*只有全部字节都发送完成才算同步成功*/ = !this.reportOffset.hasRemaining();
            // 成功之后 position == limit;
            return writeSuccess;
        }

        /**
         * // 当 diff >= msgHeaderSize 不成立 或者 diff >= (msgHeaderSize + bodySize) 不成立的时候会执行下面的代码
         *
         * byteBufferRead 最后一个桢的数据是一个半包数据的时候就会执行该方法
         *
         * reallocate：重新分配
         */
        private void reallocateByteBuffer() {
            // 0 ... dispatchPosition ............. 这一部分数据是半包数据 ..................limit/position(满了)
            // 处于 dispatchPosition 到 position 之间的字节是还没有处理的字节
            // 未处理的数量
            int remain = READ_MAX_BUFFER_SIZE/*4Mb，其实就是 byteBufferRead 的 limit */ - this.dispatchPosition;

            if (remain > 0 /* 说明最后一桢是半包数据 */) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead/*把最后一个半包数据写入到备用缓存*/);
            }

            // 交换 byteBufferRead 跟 byteBufferBackup
            this.swapByteBuffer();

            // 复原 byteBufferRead，因为已经存储了上面的半包数据
            // 后续读取数据的是从 remain 向后开始
            this.byteBufferRead.position(remain/*半包的时候不为0*/);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);

            // 恢复，因为交换之后 byteBufferRead 是一个全新的了，需要从头开始处理
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * HAClient 的核心方法
         *
         * 处理 master 发送给 slave  commitLog 数据的逻辑
         *
         * @return true:表示处理成功，false表示 socket 处于关闭状态，需要上层重新创建 haClient
         */
        private boolean processReadEvent() {

            // 控制 while 循环的一个条件变量，如果连续多次每读取到数据，则跳出循环
            int readSizeZeroTimes = 0;

            while (this.byteBufferRead.hasRemaining() /* 只要有空闲的控制就一直试图去读取数据 ，正常情况都会有空闲*/) {
                try {

                    // 加载数据到 byteBufferRead 中，返回加载到的字节数
                    int readSize = this.socketChannel.read(this.byteBufferRead/*存储master节点同步给slave节点的commitLog文件的数据*/);
                    if (readSize > 0 /*读取到数据了*/) {

                        // 读到数据了就重新计数呗
                        readSizeZeroTimes = 0;

                        // 处理从 master 发送给 slave 的 commitLog 数据的逻辑
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            ////log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0 /* 表示本次循环没有读取到数据 */) {
                        if (++readSizeZeroTimes >= 3 /*如果连续3次都没读取到数据，则结束循环*/) {
                            // 正常从这里跳出循环，并且返回 true
                            break;
                        }
                    } else {
                        //  readSize < 0，一般是 -1  的情况,表示 socket 处于关闭状态（服务端关闭了，客户端还没关闭）
                        ////log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    ////log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        /**
         * 处理 byteBufferRead 中读取到的数据
         */
        private boolean dispatchReadRequest() {

            // 每桢的数据
            // {[phyOffset1][size1][data1]}{[phyOffset2][size2][data2]}{[phyOffset3][size3][data3]}
            final int msgHeaderSize = 8/*phyoffset*/ + 4/*size*/; // phyoffset + size，协议头大小

            // 该变量记录 byteBufferRead 处理数据之前的 pos 值，用于处理完数据之后恢复 pos 指针
            // 0 ... ................. ............ position ..................limit
            int readSocketPos = this.byteBufferRead.position();

            while (true) {

                // 0 ... ................. ............ position ..................limit
                // 0 ... dispatchPosition ............. position ..................limit
                // 处于 dispatchPosition 到 position 之间的字节是还没有处理的字节

                // 表示 当前 byteBufferRead 还剩余多少 byte 没处理
                // 每处理一桢数据就会更新 dispatchPosition，让他前进一桢的长度，则 dispatchPosition 之前的数据就是已经处理过的数据
                int diff = this.byteBufferRead.position() - this.dispatchPosition /* 初始状态是 0  */;
                if (diff >= msgHeaderSize /* 条件成立，说明：byteBufferRead 内部至少有一个完整的 header 数据的  */) {
                    // 读取 header
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    // 当前 slave 端commitLog最大的物理偏移量
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            // 正常情况 二者必须相等，否则就不正常了，就要返回失败
                            // 因为读取 slave 的最大偏移量后面的数据就是需要同步的数据，那么 master 端自然也需要从 该 偏移量 开始同步数据，如果不一样，数据肯定就乱了！！！！！
                            // 一桢一桢同步，怎么会出问题？？
                            return false;
                        }
                    }

                    if (diff >= (msgHeaderSize + bodySize) /* 该条件成立，说明：当前  byteBufferRead  内部最起码包含当前桢的全部数据的，下面就是处理桢数据*/) {

                        // 用于存储桢中的数据块
                        byte[] bodyData = new byte[bodySize/*该桢数据中 commitLog 数据的大小*/];
                        // {[phyOffset1][size1][data1]}{[phyOffset2][size2][data2]}{[phyOffset3][size3][data3]}
                        // 其实就是读取 data 部分的数据
                        // 0 ... dispatchPosition ............. position ..................limit
                        this.byteBufferRead.position(this.dispatchPosition + msgHeaderSize /* 跳过头部12个字节，从 body 开始读取 */);
                        // 把数据读到数组中
                        this.byteBufferRead.get(bodyData);

                        // slave 存储数据的逻辑 ！！！！！！
                        // 把从 master 上读取到的数据写到读取 slave 节点的 commitLog 文件中
                        // master 上已经做了校验了，所以 slave 没必要再次校验
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        // 恢复指针
                        this.byteBufferRead.position(readSocketPos);

                        // 更新，加一桢数据长度，方便处理下一条数据使用
                        int frameSize = msgHeaderSize + bodySize;
                        // 0 ... dispatchPosition ............. position ..................limit
                        this.dispatchPosition = dispatchPosition + frameSize /* 让 dispatchPosition 前进一桢的长度，则 dispatchPosition 之前的数据就是已经处理过的数据 */;

                        if (!reportSlaveMaxOffsetPlus() /* 上报 slave 的同步进度 */) {
                            return false;
                        }

                        // 处理下一桢数据
                        continue;
                    }
                }

                // 当 diff >= msgHeaderSize 不成立 或者 diff >= (msgHeaderSize + bodySize) 不成立的时候会执行下面的代码
                // 如果执行到这里，则说明 byteBufferRead 中最后部分数据是半包，并且写满了
                // 意思就是 byteBufferRead 中最后一部分数据不够1桢的时候
                if (!this.byteBufferRead.hasRemaining()) {

                    // 说明 byteBufferRead 写满了
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;

            // 当前 slave 上 commitLog 的最大偏移量
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                }
            }

            return result;
        }

        /**
         * slave 节点成功连接到 master 节点才会返回 true，如果当前节点是 master 节点因为 masterAddress 是空，会返回false,再者就是连接失败的是返回 false
         *
         * @return
         * @throws ClosedChannelException
         */
        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                // master 节点 暴露 的 HA 地址端口信息，只有 slave 节点才能拿到该值，master节点是没有该值的
                String addr = this.masterAddress.get();
                if (addr != null) {
                    // slave 节点会进来这个分支

                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        // 建立连接
                        this.socketChannel = RemotingUtil.connect/*通过master节点的地址，slave节点主动发起连接*/(socketAddress);
                        if (this.socketChannel != null) {
                            // 注册到多路复用器 关注 OP_READ 事件，读取 master 端发送来的数据
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }
                // 获取当前 slave 节点的 commitLog 上的最大物理偏移量
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
                // 更新时间，避免误关闭连接
                this.lastWriteTimestamp = System.currentTimeMillis();
            }
            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {
                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }
                    this.socketChannel.close();
                    this.socketChannel = null;
                } catch (IOException e) {
                    //log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        /**
         * slave节点会执行
         * 1.每5秒上报当前进度给master
         * 2.每秒中读取master节点发生过来的数据
         */
        @Override
        public void run() {
            while (!this.isStopped()) {
                try {
                    if (this.connectMaster() /* 当前slave节点与master节点成功连接的时候返回true */) {
                        if (this.isTimeToReportOffset()/*上次上报的时间已经超过5秒了，就需要再次上报进度到 master*/) {
                            // 上报 slave 同步进度到 master
                            boolean result/*上报结果*/ = this.reportSlaveMaxOffset(this.currentReportedOffset/*首次跟master连接的时候就会更新该字段：this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();*/);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        // 阻塞 1 s，因为该连接注册的是读事件，读到数据之后就会往下执行
                        this.selector.select(1000);

                        /**
                         * 执行到这里，又2个情况：
                         * 1. socketChannel 的 OP_READ 就绪
                         * 2.多路复用器的select 方法超时1秒
                         */

                        // HAClient 的核心方法
                        boolean ok = this.processReadEvent()/*只要 byteBufferRead 还有空闲就会一直循环从master节点读取数据*/;
                        if (!ok) {
                            this.closeMaster();
                        }

                        // TODO ？
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }
                        long interval = HAService.this.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()/*20秒*/) {
                            // 如果 超过 20秒都是空闲，则清理该连接
                            this.closeMaster();
                        }
                    } else {

                        // slave 连接 master 失败，则等待一会
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    this.waitForRunning(1000 * 5);
                }
            }
        }
    }
}

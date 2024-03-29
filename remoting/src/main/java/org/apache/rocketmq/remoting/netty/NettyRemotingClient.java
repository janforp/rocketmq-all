package org.apache.rocketmq.remoting.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.io.IOException;
import java.net.SocketAddress;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("all")
public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    // 客户端网络层配置
    private final NettyClientConfig nettyClientConfig;

    // 客户端网络层启对象，通过它 bootstrap.connect(addr) 会得到一个 NioSocketChannel， 接下来使用该ch可以与对端完成通信
    private final Bootstrap bootstrap = new Bootstrap();

    // 客户端网络层 Netty IO 线程组
    private final EventLoopGroup eventLoopGroupWorker;

    /**
     * 控制下面的映射表的读写
     *
     * @see NettyRemotingClient#channelTables 控制该对象
     */
    private final Lock lockChannelTables = new ReentrantLock();

    /**
     * 维护通信通道
     *
     * key:服务器地址
     * value:客户端与服务器连接的 channel 封装对象
     */
    private final ConcurrentMap<String /* 服务器addr */, ChannelWrapper/*客户端 跟 服务端连接封装*/> channelTables = new ConcurrentHashMap<String, ChannelWrapper>();

    private final Timer timer = new Timer("ClientHouseKeepingService", true);

    // namesrv 地址 配置
    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<List<String>>();

    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<String>();

    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());

    private final Lock lockNamesrvChannel = new ReentrantLock();

    // 公共线程池
    private final ExecutorService publicExecutor;

    /**
     * Invoke the callback methods in this executor when process response.
     * 回调线程池：客户端发起！！！异步！！！请求，服务器响应数据的时候由该线程池处理
     */
    @Setter
    private ExecutorService callbackExecutor;

    // 客户端这里是 null
    @Getter
    private final ChannelEventListener channelEventListener;

    // Channel Pipeline 内的 handler 使用的线程资源

    /**
     * pipeline.addLast(defaultEventExecutorGroup,handler...)
     *
     * netty 处理器使用的线程池，避免占用netty的IO线程执行业务逻辑
     */
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    /**
     * @param nettyClientConfig 配置
     * @param channelEventListener 其实 null
     */
    public NettyRemotingClient(final NettyClientConfig nettyClientConfig, final ChannelEventListener channelEventListener) {
        // 赋值而已，2个并发控制变量
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads()/* cpu 核数 */;
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {

            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });

        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {

            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
            }
        });

        tls();
    }

    private void tls() {
        if (nettyClientConfig.isUseTLS()) {
            try {
                sslContext = TlsHelper.buildSslContext(true);
                log.info("SSL enabled for client");
            } catch (IOException e) {
                log.error("Failed to create SSLContext", e);
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext", e);
                throw new RuntimeException("Failed to create SSLContext", e);
            }
        }
    }

    private static int initValueIndex() {
        Random r = new Random();
        return Math.abs(r.nextInt() % 999) % 999;
    }

    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyClientConfig.getClientWorkerThreads()/*4*/, new ThreadFactory() {

            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
            }
        });

        // 配置 netty 客户端启动类对象
        this.bootstrap
                .group(this.eventLoopGroupWorker)// 一个线程的线程池

                .channel(NioSocketChannel.class) // ch 类型：Nio

                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis()) // 超时时间
                .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize()) // 发送缓冲区大小
                .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize()) // 接收缓冲区大小
                // 初始化 pipeline
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        if (nettyClientConfig.isUseTLS()) {
                            if (null != sslContext) {
                                pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
                                log.info("Prepend SSL handler");
                            } else {
                                log.warn("Connections are insecure as SSLContext is null!");
                            }
                        }

                        // 添加几个处理器
                        pipeline.addLast(defaultEventExecutorGroup,
                                new NettyEncoder(),/*RemotingCommand 编码为字节*/
                                new NettyDecoder(),/* 字节 解码为 RemotingCommand 对象 */
                                new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),// 监听当前通道的空闲状态
                                new NettyConnectManageHandler(), // 监听通道是否关闭，如果关闭需要发送通道事件
                                /**
                                 * 核心业务逻辑
                                 *
                                 * @see NettyRemotingAbstract#processMessageReceived(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)
                                 */
                                new NettyClientHandler());
                    }
                });

        /**
         * TODO 注意：bootstrap 仅仅是配置好了 客户端的数据，在这里并没有创建任何的 ch 对象，只有在发起请求的时候才建立连接
         *
         * @see NettyRemotingClient#getAndCreateChannel(java.lang.String)
         */

        // 定时扫描 ResponseFutureTable 中超时的 ResponseFuture，避免客户端线程 长时间 阻塞
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);

        // 客户端这里是null
        if (this.channelEventListener != null) {
            // 客户端不会启动
            this.nettyEventExecutor.start();
        }
    }

    @Override
    public void shutdown() {
        try {

            // 先取消定时任务
            this.timer.cancel();

            for (ChannelWrapper cw : this.channelTables.values()) {
                this.closeChannel(null, cw.getChannel());
            }

            this.channelTables.clear();

            this.eventLoopGroupWorker.shutdownGracefully();

            this.nettyEventExecutor.shutdown();

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingClient shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    public void closeChannel(final String addr, final Channel channel) {
        if (null == channel) {
            return;
        }

        // 通过channel解析出远程的地址
        String channelRemoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
        // 以传入的 addr 为优先
        final String addrRemote = (null == addr ? channelRemoteAddr : addr);

        try {
            //ReentrantLock.tryLock
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTables.get(addrRemote);
                    if (null == prevCW) {
                        removeItemFromTable = false;
                    } else if (prevCW.getChannel() != channel) {
                        removeItemFromTable = false;
                    }
                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                    }
                    RemotingUtil.closeChannel(channel);
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }

    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        if (rpcHook != null && !rpcHooks.contains(rpcHook)) {
            rpcHooks.add(rpcHook);
        }
    }

    public void closeChannel(final Channel channel) {
        if (null == channel) {
            return;
        }
        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    String addrRemote = null;
                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {
                        String key = entry.getKey();
                        ChannelWrapper prev = entry.getValue();
                        if (prev.getChannel() != null) {
                            if (prev.getChannel() == channel) {
                                prevCW = prev;
                                addrRemote = key;
                                break;
                            }
                        }
                    }

                    if (null == prevCW) {
                        log.info("eventCloseChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        RemotingUtil.closeChannel(channel);
                    }
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }

    /**
     * @param addrs
     * @see org.apache.rocketmq.client.impl.factory.MQClientInstance#MQClientInstance(org.apache.rocketmq.client.ClientConfig, int, java.lang.String, org.apache.rocketmq.remoting.RPCHook)
     */
    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        if (addrs.isEmpty()) {
            return;
        }

        // AtomicReference<List<String>> namesrvAddrList = new AtomicReference<List<String>>();
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if (null == old) {
            // 之前就是空的，则以本次为准
            update = true;
        } else if (addrs.size() != old.size() /* namesrv 地址的数量发生了变化 */) {
            update = true;
        } else {
            // 数量没有发生变化，则进一步检查是否其中的某个发生了变化
            for (int i = 0; i < addrs.size() && !update; i++) {
                if (!old.contains(addrs.get(i))) {
                    update = true;
                }
            }
        }

        if (update) {
            Collections.shuffle(addrs);
            log.info("name server address updated. NEW : {} , OLD: {}", addrs, old);
            this.namesrvAddrList.set(addrs);
        }
    }

    /**
     * @param addr 服务端地址
     * @param request 网络层传输对象，里面封装了请求数据
     * @param timeoutMillis 超时时间
     */
    @Override
    public RemotingCommand invokeSync(String addr/*服务端地址*/, final RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        // 开始时间
        long beginStartTime = System.currentTimeMillis();
        // 拿到 netty 链接
        // 获取 或者 创建 客户端或者服务端与指定地址 addr 的 ch
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()/*客户端与服务端 channel 通道正常*/) {
            // 如果 channel.isActive() == true 说明通道正常，可以通信
            try {
                //钩子
                doBeforeRpcHooks(addr, request);

                // 计算当前耗时
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    // 超时则抛异常
                    throw new RemotingTimeoutException("invokeSync call timeout");
                }
                // 执行远程调用
                RemotingCommand response/*服务端的响应数据*/ = this.invokeSyncImpl/*调用父类的实现*/(channel, request, timeoutMillis - costTime);
                String remoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
                // 执行钩子，扩展点
                doAfterRpcHooks(remoteAddr, request, response);
                return response;
            } catch (RemotingSendRequestException e) {
                log.warn("invokeSync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            } catch (RemotingTimeoutException e) {
                if (nettyClientConfig.isClientCloseSocketIfTimeout()) {
                    this.closeChannel(addr, channel);
                    log.warn("invokeSync: close socket because of timeout, {}ms, {}", timeoutMillis, addr);
                }
                log.warn("invokeSync: wait response timeout exception, the channel[{}]", addr);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * @param addr localhost:8888
     */
    private Channel getAndCreateChannel(final String addr) throws RemotingConnectException, InterruptedException {
        if (null == addr) {
            return getAndCreateNameserverChannel();
        }

        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }

        return this.createChannel(addr);
    }

    private Channel getAndCreateNameserverChannel() throws RemotingConnectException, InterruptedException {
        // 假设是第一次

        String addr/*第一次肯定 null*/ = this.namesrvAddrChoosed.get();
        if (addr != null) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw != null && cw.isOK()) {
                return cw.getChannel();
            }
        }

        final List<String> addrList = this.namesrvAddrList.get();
        if (this.lockNamesrvChannel.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                addr/*第一次肯定 null*/ = this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper cw = this.channelTables.get(addr);
                    if (cw != null && cw.isOK()) {
                        return cw.getChannel();
                    }
                }

                if (addrList != null && !addrList.isEmpty()) {
                    for (int i = 0; i < addrList.size(); i++) /*遍历所有服务器地址*/ {

                        // AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
                        int index = this.namesrvIndex.incrementAndGet();
                        index = Math.abs(index);
                        index = index % addrList.size();

                        // 随机选取一个服务端地址
                        String newAddr = addrList.get(index);

                        // 随机选取一个服务端地址
                        this.namesrvAddrChoosed.set(newAddr);
                        log.info("new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}", addr, newAddr, namesrvIndex);
                        Channel channelNew = this.createChannel(newAddr);
                        if (channelNew != null) {
                            return channelNew;
                        }
                    }
                    throw new RemotingConnectException(addrList.toString());
                }
            } finally {
                this.lockNamesrvChannel.unlock();
            }
        } else {
            log.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    private Channel createChannel(final String addr /*服务器地址*/) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            // 连接还有效
            return cw.getChannel();
        }

        // 该地址的连接已经失效了，则需要重新建立连接
        boolean tryLock = this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        if (tryLock) {
            try {
                //是否新建连接
                boolean createNewConnection;
                cw = this.channelTables.get(addr);
                if (cw != null) {
                    if (cw.isOK()) {
                        return cw.getChannel();
                    } else if (!cw.getChannelFuture().isDone()) {

                        // TODO ??
                        createNewConnection = false;
                    } else {

                        // addr 的上一个连接已经断开了，需要从映射表中移除
                        this.channelTables.remove(addr);
                        createNewConnection = true;
                    }
                } else {

                    // 如果该 addr 之前没有创建了连接，则本次肯定是新建
                    createNewConnection = true;
                }
                if (createNewConnection) {
                    SocketAddress socketAddress = RemotingHelper.string2SocketAddress(addr);
                    //io.netty.bootstrap.Bootstrap

                    // 发起请求连接！！！
                    ChannelFuture channelFuture = this.bootstrap/*客户端的启动对象*/.connect(socketAddress);
                    log.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                    cw = new ChannelWrapper(channelFuture);
                    this.channelTables.put(addr, cw);
                }
            } catch (Exception e) {
                log.error("createChannel: create channel exception", e);
            } finally {
                this.lockChannelTables.unlock();
            }
        } else {
            log.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        if (cw != null) {
            ChannelFuture channelFuture = cw.getChannelFuture();
            int connectTimeoutMillis = this.nettyClientConfig.getConnectTimeoutMillis();
            // 等待一个超时时间
            if (channelFuture.awaitUninterruptibly(connectTimeoutMillis)) {
                if (cw.isOK()) {
                    log.info("createChannel: connect remote host[{}] success, {}", addr, channelFuture.toString());
                    return cw.getChannel();
                } else {
                    log.warn("createChannel: connect remote host[" + addr + "] failed, " + channelFuture, channelFuture.cause());
                }
            } else {
                // 超时了
                log.warn("createChannel: connect remote host[{}] timeout {}ms, {}", addr, connectTimeoutMillis, channelFuture.toString());
            }
        }
        return null;
    }

    @Override
    @SneakyThrows
    public void invokeAsync(String addr/*服务端地址*/, RemotingCommand request/*请求数据*/, long timeoutMillis, InvokeCallback invokeCallback) {
        long beginStartTime = System.currentTimeMillis();
        // 拿到跟服务端的连接
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            doBeforeRpcHooks(addr, request);
            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeoutMillis < costTime) {
                throw new RemotingTooMuchRequestException("invokeAsync call timeout");
            }
            this.invokeAsyncImpl/*调用父类的实现*/(channel, request, timeoutMillis - costTime, invokeCallback);
        } else {
            // 网络连接异常
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                doBeforeRpcHooks(addr, request);
                this.invokeOnewayImpl/*调用父类的实现*/(channel, request, timeoutMillis);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeOneway: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void registerProcessor(int requestCode/*业务代码*/, NettyRequestProcessor processor/*业务处理器*/, ExecutorService executor/*处理业务逻辑的时候，在该线程池资源中执行*/) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            // 没传就使用公共的线程池
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public boolean isChannelWritable(String addr) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.isWritable();
        }
        return true;
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return callbackExecutor != null ? callbackExecutor : publicExecutor;
    }

    /**
     * 包装 ChannelFuture
     *
     * 服务端跟客户端连接封装
     */
    @AllArgsConstructor
    static class ChannelWrapper {

        @Getter
        private final ChannelFuture channelFuture;

        public boolean isOK() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        public boolean isWritable() {
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
            // 核心函数
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            log.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.channel()));
            }
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, promise);
            NettyRemotingClient.this.failFast(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }
        }
    }
}

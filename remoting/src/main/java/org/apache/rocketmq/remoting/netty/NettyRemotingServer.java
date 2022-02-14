package org.apache.rocketmq.remoting.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import lombok.Getter;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 主要负责网络层的调用
 * 报文的编解码
 *
 * @see RemotingCommand 网络层传输协议对象
 */

@SuppressWarnings("all")
public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    // netty服务端启动对象
    private final ServerBootstrap serverBootstrap;

    // netty worker 组线程池
    private final EventLoopGroup eventLoopGroupSelector;

    // netty boss 线程池，一般只有一个线程
    private final EventLoopGroup eventLoopGroupBoss;

    // NETty 服务端网络配置，配置文件决定
    private final NettyServerConfig nettyServerConfig;

    // 公共线程池，注册处理器时，如果未指定线程池，则使用该线程池
    private final ExecutorService publicExecutor;

    /**
     * @see org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService namesrv使用
     * @see org.apache.rocketmq.broker.client.ClientHousekeepingService broker 使用,监听客户端的连接状态
     */
    @Getter
    private final ChannelEventListener channelEventListener;

    /**
     * 定时任务，支持 scanResponseFuture 任务，负责清理过期的 ResponseFuture
     *
     * @see NettyRemotingAbstract#scanResponseTable()
     * @see NettyRemotingAbstract#responseTable 清理过期的数据
     */
    private final Timer timer = new Timer("ServerHouseKeepingService", true);

    // 当向 channel pipeline 添加 handler 时，制定了 GROUP（线程组） 时候，网络事件传播到当前 handler 时，事件处理器由分配给 handler 的线程执行
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    // 服务器绑定端口,启动成功后赋值
    private int port = 0;

    private static final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";

    private static final String TLS_HANDLER_NAME = "sslHandler";

    private static final String FILE_REGION_ENCODER_NAME = "fileRegionEncoder";

    // sharable handlers
    private HandshakeHandler handshakeHandler;

    /**
     * 通用编码器
     *
     * @see RemotingCommand
     */
    private NettyEncoder encoder;

    // 连接管理处理器
    private NettyConnectManageHandler connectionManageHandler;

    /**
     * 真正的业务逻辑处理器
     *
     * 当 broker 或者 namesrv 服务器收到消息的时候，会经过这个处理器
     *
     * @see NettyRemotingAbstract#processMessageReceived(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)
     */
    private NettyServerHandler serverHandler;

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }

    /**
     * broker 跟 namesrv 都会给自实例话一个 NettyRemotingServer 对象
     *
     * @param nettyServerConfig 配置
     * @param channelEventListener 监听器
     * @see org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService
     * @see org.apache.rocketmq.broker.client.ClientHousekeepingService
     */
    public NettyRemotingServer(final NettyServerConfig nettyServerConfig, final ChannelEventListener channelEventListener) {
        // 服务器向 客户端主动发起请求时的并发限制
        super(nettyServerConfig.getServerOnewaySemaphoreValue()/*单向请求的并发限制*/, nettyServerConfig.getServerAsyncSemaphoreValue()/*异步请求的并发限制*/);
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;

        // 公共线程池的线程数量，默认0，修改为4
        /*
         * ##
         * # 名称：NettyServerConfig.serverCallbackExecutorThreads <int>
         * # 默认值：0 <在源程序中初始化字段时指定>
         * # 描述：Netty public 任务线程池线程个数，Netty 网络设计，根据业务类型会创建不同的线程池，比如处理发送消息、消息消费、心跳检测等。如果该业务类型(RequestCode)未注册线程池，则由 public 线程池执行
         * # 建议：
         * ##
         * serverCallbackExecutorThreads=0
         */
        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        // 创建公共线程池
        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyServerPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });

        // 创建 boss,work 线程组
        if (useEpoll()) {
            this.eventLoopGroupBoss = new EpollEventLoopGroup(1, new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyEPOLLBoss_%d", this.threadIndex.incrementAndGet()));
                }
            });

            this.eventLoopGroupSelector = new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                private final int threadTotal = nettyServerConfig.getServerSelectorThreads();

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerEPOLLSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });
        } else {
            this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyNIOBoss_%d", this.threadIndex.incrementAndGet()));
                }
            });

            this.eventLoopGroupSelector = new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                private final int threadTotal = nettyServerConfig.getServerSelectorThreads();

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerNIOSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });
        }

        // 创建完，但是并没有启动

        loadSslContext();
    }

    public void loadSslContext() {
        TlsMode tlsMode = TlsSystemConfig.tlsMode;
        log.info("Server is running in TLS {} mode", tlsMode.getName());

        if (tlsMode != TlsMode.DISABLED) {
            try {
                sslContext = TlsHelper.buildSslContext(false);
                log.info("SSLContext created for server");
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext for server", e);
            } catch (IOException e) {
                log.error("Failed to create SSLContext for server", e);
            }
        }
    }

    private boolean useEpoll() {
        return RemotingUtil.isLinuxPlatform()
                && nettyServerConfig.isUseEpollNativeSelector()
                && Epoll.isAvailable();
    }

    /**
     * 启动 netty 服务端
     * 最后会开一个定时器定时扫描超时的请求.扫描的原理就是就是根据请求开始时间与目前时间的间隔差,如果超过最大阀值就通过线程池去异步销毁.
     */
    @Override
    public void start() {
        // 当向 channel pipeline 添加 handler 时，制定了 GROUp时候，网络事件传播到当前 handler 时，事件处理器由分配给 handler 的线程执行
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyServerConfig.getServerWorkerThreads(),/*配置的线程数量 8 */
                // 线程工厂
                new ThreadFactory() {
                    private final AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyServerCodecThread_" + this.threadIndex.incrementAndGet());
                    }
                });

        // 创建handler对象，下面需要使用
        prepareSharableHandlers();

        // 配置服务端启动对象
        ServerBootstrap childHandler =
                this.serverBootstrap/*服务端启动对象*/.group(this.eventLoopGroupBoss/*boss*/, this.eventLoopGroupSelector/*worker*/)

                        // 优先使用 epoll
                        .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)

                        // 服务端通道选项
                        .option(ChannelOption.SO_BACKLOG, 1024)
                        .option(ChannelOption.SO_REUSEADDR, true)
                        .option(ChannelOption.SO_KEEPALIVE, false)
                        .childOption(ChannelOption.TCP_NODELAY, true)

                        // 客户端通道选项
                        .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize())
                        .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize())

                        // 服务端监听的端口
                        .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))

                        // 当客户端发起请求连接服务端的时候就会调到这里面
                        .childHandler(new ChannelInitializer<SocketChannel>() {

                            // 当客户端发起请求连接服务端的时候就会调到这里面
                            @Override
                            public void initChannel(SocketChannel ch) {
                                ch.pipeline()
                                        // ChannelPipeline addLast(EventExecutorGroup group, String name, ChannelHandler handler);
                                        .addLast(defaultEventExecutorGroup/*指定该handler使用的线程组*/, HANDSHAKE_HANDLER_NAME/*handshakeHandler*/, handshakeHandler/*数字证书相关，安全*/) // 指定名称的处理器，该处理器工作在指定的线程中
                                        // ChannelPipeline addLast(EventExecutorGroup group, ChannelHandler... handlers);
                                        .addLast(defaultEventExecutorGroup,// 这些理器工作在指定的线程中
                                                encoder, // 编码 RemotingCommand
                                                new NettyDecoder(), // 解码 RemotingCommand
                                                new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()/*120*/),// 监听当前通道的空闲时间
                                                /**
                                                 * 连接管理处理器，监听通道状态发送改变，如关闭，连接，异常等
                                                 * @see NettyRemotingAbstract#nettyEventExecutor
                                                 */
                                                connectionManageHandler,
                                                serverHandler // 真正的业务逻辑处理器
                                        );
                            }
                        });

        if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
            // 开启 netty 内存池
            PooledByteBufAllocator pooledByteBufAllocator = PooledByteBufAllocator.DEFAULT;
            childHandler.childOption(ChannelOption.ALLOCATOR, pooledByteBufAllocator);
        }

        try {
            // 启动服务器
            ChannelFuture channelFuture = this.serverBootstrap.bind();
            ChannelFuture sync = channelFuture.sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            this.port = addr.getPort();
        } catch (InterruptedException e1) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
        }

        /**
         * @see org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService namesrv使用
         * @see org.apache.rocketmq.broker.client.ClientHousekeepingService broker 使用,监听客户端的连接状态
         */
        if (this.channelEventListener != null) {
            // 这个任务中会真正的使用这个监听器
            this.nettyEventExecutor.start();
        }

        // 最后会开一个定时器定时扫描超时的请求.扫描的原理就是就是根据请求开始时间与目前时间的间隔差,如果超过最大阀值就通过线程池去异步销毁.
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    // 扫描过期请求
                    NettyRemotingServer.this.scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }

    @SuppressWarnings("all")
    @Override
    public void shutdown() {
        try {
            if (this.timer != null) {
                // 取消任务
                this.timer.cancel();
            }

            this.eventLoopGroupBoss.shutdownGracefully();
            this.eventLoopGroupSelector.shutdownGracefully();

            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingServer shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        if (rpcHook != null && !rpcHooks.contains(rpcHook)) {
            rpcHooks.add(rpcHook);
        }
    }

    @Override
    public void registerProcessor(int requestCode/*业务代码*/, NettyRequestProcessor processor/*业务处理器*/, ExecutorService executor/*处理业务逻辑的时候，在该线程池资源中执行*/) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor/*业务处理器*/, executorThis/*处理业务逻辑的时候，在该线程池资源中执行*/);
        // 处理器映射表
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair<NettyRequestProcessor, ExecutorService>(processor, executor);
    }

    @Override
    public int localListenPort() {
        return this.port;
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return processorTable.get(requestCode);
    }

    /**
     * 服务器主动 主动想客户端发起请求的时候使用的方法
     *
     * @param channel ch
     * @param request 请求
     * @param timeoutMillis 超时时长
     * @return 请求？
     */
    @Override
    public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        return this.invokeSyncImpl(channel, request, timeoutMillis);
    }

    /**
     * 服务器主动 主动想客户端发起请求的时候使用的方法
     *
     * @param channel ch
     * @param request 请求
     * @param timeoutMillis 超时时长
     * @param invokeCallback 请求回调处理对象
     */
    @Override
    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
    }

    @Override
    public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        // 请求发送完成即可，不关心结果
        this.invokeOnewayImpl(channel, request, timeoutMillis);
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    private void prepareSharableHandlers() {
        handshakeHandler = new HandshakeHandler(TlsSystemConfig.tlsMode /* permissive */);
        encoder = new NettyEncoder();
        connectionManageHandler = new NettyConnectManageHandler();
        serverHandler = new NettyServerHandler();
    }

    /**
     * 数字证书相关，安全
     */
    @ChannelHandler.Sharable
    class HandshakeHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private final TlsMode tlsMode;

        private static final byte HANDSHAKE_MAGIC_CODE = 0x16;

        HandshakeHandler(TlsMode tlsMode) {
            this.tlsMode = tlsMode;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {

            // mark the current position so that we can peek the first byte to determine if the content is starting with
            // TLS handshake
            msg.markReaderIndex();

            byte b = msg.getByte(0);

            if (b == HANDSHAKE_MAGIC_CODE) {
                switch (tlsMode) {
                    case DISABLED:
                        ctx.close();
                        log.warn("Clients intend to establish an SSL connection while this server is running in SSL disabled mode");
                        break;
                    case PERMISSIVE:
                    case ENFORCING:
                        if (null != sslContext) {
                            ctx.pipeline()
                                    .addAfter(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME, TLS_HANDLER_NAME, sslContext.newHandler(ctx.channel().alloc()))
                                    .addAfter(defaultEventExecutorGroup, TLS_HANDLER_NAME, FILE_REGION_ENCODER_NAME, new FileRegionEncoder());
                            log.info("Handlers prepended to channel pipeline to establish SSL connection");
                        } else {
                            ctx.close();
                            log.error("Trying to establish an SSL connection but sslContext is null");
                        }
                        break;

                    default:
                        log.warn("Unknown TLS mode");
                        break;
                }
            } else if (tlsMode == TlsMode.ENFORCING) {
                ctx.close();
                log.warn("Clients intend to establish an insecure connection while this server is running in SSL enforcing mode");
            }

            // reset the reader index so that handshake negotiation may proceed as normal.
            msg.resetReaderIndex();

            try {
                // Remove this handler
                ctx.pipeline().remove(this);
            } catch (NoSuchElementException e) {
                log.error("Error while removing HandshakeHandler", e);
            }

            // Hand over this message to the next .
            ctx.fireChannelRead(msg.retain());
        }
    }

    @ChannelHandler.Sharable
    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg/*该对象是 org.apache.rocketmq.remoting.netty.NettyDecoder 解码器转化过来的，这就是 netty 的使用方法！！！*/) {

            // 对端有数据过来啦啊
            processMessageReceived(ctx, msg);
        }
    }

    /**
     * 双向通道
     */
    @ChannelHandler.Sharable
    class NettyConnectManageHandler extends ChannelDuplexHandler {

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelRegistered {}", remoteAddress);
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddress);
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]", remoteAddress);
            super.channelActive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]", remoteAddress);
            super.channelInactive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]", remoteAddress);
                    RemotingUtil.closeChannel(ctx.channel());
                    if (NettyRemotingServer.this.channelEventListener != null) {
                        NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }
            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }

            RemotingUtil.closeChannel(ctx.channel());
        }
    }
}

package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.common.ServiceThread;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingCommandType;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 服务端网络层跟客户端网络层都继承自该类
 */
@SuppressWarnings("all")
public abstract class NettyRemotingAbstract {

    /**
     * Remoting logger instance.
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * 单向请求的并发限制
     * Semaphore to limit maximum number of on-going one-way requests, which protects system memory footprint.
     */
    protected final Semaphore semaphoreOneway;

    /**
     * 异步调用信号量
     * Semaphore to limit maximum number of on-going asynchronous requests, which protects system memory footprint.
     */
    protected final Semaphore semaphoreAsync;

    /**
     * This map caches all on-going requests. -- 此映射缓存所有正在进行的请求。
     * 请求响应映射表
     * 如果服务端发起请求到客户端，则服务端会有这样一个映射表 {@link NettyRemotingServer} 服务端实现类
     * 如果客户端发起请求到服务端，则客户端会有这样一个映射表 {@link NettyRemotingClient} 客户端实现类
     * 主要目的是待对端响应的时候跟唯一opaque能够找到响应到哪个对象
     *
     * 因为发起请求跟得到响应，两个步骤之间是有一定时间间隔的！
     *
     * @see RemotingCommand#requestId
     * @see NettyRemotingAbstract#processResponseCommand(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)
     * @see NettyRemotingAbstract#invokeSyncImpl(io.netty.channel.Channel, org.apache.rocketmq.remoting.protocol.RemotingCommand, long)
     */
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture/*响应，里面封装了异步转同步的逻辑*/> responseTable = new ConcurrentHashMap<Integer, ResponseFuture>(256);

    /**
     * This container holds all processors per request code, aka, for each incoming request, we may look up the responding processor in this map to handle the request.
     * 这个容器包含每个请求代码的所有处理器，也就是对于每个传入的请求，我们可以在这个映射中查找响应处理器来处理请求。
     */
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor/*业务处理器*/, ExecutorService/*处理业务逻辑的时候，在该线程池资源中执行*/>> processorTable = new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);

    /**
     * Executor to feed netty events to user defined {@link ChannelEventListener}.
     */
    protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    /**
     * The default request processor to use in case there is no exact match in {@link #processorTable} per request code.
     */
    protected Pair<NettyRequestProcessor, ExecutorService/*处理器处理业务的时候使用的线程池*/> defaultRequestProcessor;

    /**
     * SSL context via which to create {@link SslHandler}.
     */
    protected volatile SslContext sslContext;

    /**
     * custom rpc hooks
     */
    @Getter
    protected List<RPCHook> rpcHooks = new ArrayList<RPCHook>();

    static {
        NettyLogger.initNettyLogger();
    }

    /**
     * Constructor, specifying capacity of one-way and asynchronous semaphores.
     * <p>
     * // 服务器向 客户端主动发起请求时的并发限制
     * //1.单向请求的并发限制
     * //2.异步请求的并发限制
     *
     * @param permitsOneway Number of permits for one-way requests.
     * @param permitsAsync Number of permits for asynchronous requests.
     */
    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    /**
     * Put a netty event to the executor.
     *
     * @param event Netty event instance.
     * @see NettyRemotingServer.NettyConnectManageHandler 事件分发的地方
     */
    public void putNettyEvent(final NettyEvent event) {
        // 添加到 netty 事件队列中
        this.nettyEventExecutor.putNettyEvent(event);
    }

    /**
     * Entry of incoming command processing.
     *
     * <p>
     * <strong>Note:</strong>
     * The incoming remoting command may be
     * <ul>
     * <li>An inquiry request from a remote peer component;</li>
     * <li>A response to a previous request issued by this very participant.</li>
     * </ul>
     * </p>
     *
     * @param ctx Channel handler context.
     * @param msg incoming remoting command.
     * @see NettyRemotingClient.NettyClientHandler
     * @see NettyRemotingServer.NettyServerHandler
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
        if (msg != null) {

            // 消息类型是请求还是响应
            RemotingCommandType type = msg.getType();
            switch (type) {
                case REQUEST_COMMAND: // 客户端发起的请求
                    processRequestCommand(ctx, msg);
                    break;
                case RESPONSE_COMMAND: // 客户端响应给服务端的结果
                    processResponseCommand(ctx, msg);
                    break;
                default:
                    break;
            }
        }
    }

    protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doBeforeRequest(addr, request);
            }
        }
    }

    protected void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doAfterResponse(addr, request, response);
            }
        }
    }

    /**
     * Process incoming request command issued by remote peer.
     * <p>
     * 客户端发起的请求
     *
     * @param ctx channel handler context.
     * @param cmd request command.
     */
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        // 根据业务代码找到合适的处理器和线程池资源---pair
        int code = cmd.getCode();
        final Pair<NettyRequestProcessor/*业务处理器*/, ExecutorService/*处理线程池*/> matched = this.processorTable.get(code);
        // 如果没找到，则使用默认的
        /**
         * @see org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor namesrv使用这个处理器
         */
        final Pair<NettyRequestProcessor, ExecutorService> pair = (null == matched) ? this.defaultRequestProcessor/*未注册处理器，则使用默认处理器，namesrv就是使用默认处理器*/ : matched;
        final String remoteAddr/* 通过channel解析出远程的地址 */ = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
        // 拿到请求id
        final int opaque = cmd.getOpaque();
        if (pair == null) {
            String error = " request type " + code + " not supported";
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setOpaque(opaque);
            ctx.writeAndFlush(response);
            log.error(remoteAddr + error);
            return;
        }
        // 核心逻辑 start
        final NettyRequestProcessor nettyRequestProcessor = pair.getObject1();
        Runnable run = new Runnable() {
            @Override
            public void run() {
                try {
                    // 执行rpc调用前的钩子函数
                    doBeforeRpcHooks(remoteAddr /* 通过channel解析出远程的地址 */, cmd);

                    /**
                     * 如果是异步的，任务执行完成之后会回调到该对象的 callback 函数
                     *
                     * @see AsyncNettyRequestProcessor#asyncProcessRequest 这个方法中调用回调
                     */
                    final RemotingResponseCallback callback = new RemotingResponseCallback() {
                        @Override
                        public void callback(RemotingCommand response) {
                            // 执行rpc调用后的钩子函数
                            doAfterRpcHooks(remoteAddr/* 通过channel解析出远程的地址 */, cmd, response);
                            if (cmd.isOnewayRPC()) {
                                // 是否时单向的，如果时，则不响应了
                                return;
                            }

                            // 进入这里则说明时需要返回结果的
                            if (response != null) {
                                // 设置请求时候来的请求id，客户端根据请求id在 responseFutureTable 中找到 responseFuture 完成结果的交互
                                response.setOpaque(opaque);
                                // 设置为响应类型
                                response.markResponseType();
                                try {
                                    // 真正的在网络层响应,交给netty做 IO线程做,返回给对端
                                    ctx.writeAndFlush(response);
                                } catch (Throwable e) {
                                    // ignore
                                }
                            }
                        }
                    };

                    // 获取到处理器
                    final NettyRequestProcessor requestProcessor = nettyRequestProcessor;
                    if (requestProcessor instanceof AsyncNettyRequestProcessor/*异步*/) {
                        // namesrv 进入这里
                        AsyncNettyRequestProcessor processor = (AsyncNettyRequestProcessor) requestProcessor;
                        /**
                         * 以 namesrv 注册 broker 为例
                         * 执行任务，不返回响应，而是通过异步的方式
                         * @see org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor namesrv 使用的处理器
                         * @see AsyncNettyRequestProcessor#asyncProcessRequest namesrv首先调用这个方法
                         * @see org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor#processRequest namesrv然后调用这个方法
                         */
                        processor.asyncProcessRequest(ctx, cmd, callback);
                    } else {
                        // 如果是同步的，则执行这个分支
                        // 执行任务，拿到响应
                        RemotingCommand response = requestProcessor.processRequest(ctx, cmd);
                        // 执行rpc调用后的钩子函数
                        doAfterRpcHooks(remoteAddr/* 通过channel解析出远程的地址 */, cmd, response);
                        // 同步的任务，当前线程自己主动调用 callback，把结果写到对端
                        callback.callback(response);
                    }
                } catch (Throwable e) {
                    if (!cmd.isOnewayRPC()) {
                        // 如果不是单向请求，则发生异常的时候也需要响应给对端
                        final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, RemotingHelper.exceptionSimpleDesc(e));
                        response.setOpaque(opaque);
                        ctx.writeAndFlush(response);
                    }
                }
            }
        };
        // 核心逻辑 over

        // Pair<NettyRequestProcessor, ExecutorService>
        if (nettyRequestProcessor.rejectRequest()) {
            // 如果拒绝了请求
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY, "[REJECTREQUEST]system busy, start flow control for a while");
            response.setOpaque(opaque);
            // 通过 netty 写入到对端
            ctx.writeAndFlush(response);
            return;
        }

        // 没有拒绝，则提交到线程池执行任务
        try {
            // 将 runnable 和 ch 以及 请求 cmd 封装起来
            final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
            // 提交到线程池，里面会执行上面封装的 run 方法
            ExecutorService executorService = pair.getObject2();
            executorService.submit(requestTask);
        } catch (RejectedExecutionException e/*线程池任务队列满了，被拒绝了*/) {
            if (!cmd.isOnewayRPC()) {
                // 如果该请求不是单向的，则至少给对端一个响应
                final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY, "[OVERLOAD]system busy, start flow control for a while");
                response.setOpaque(opaque);
                // 通过 netty 写入到对端
                ctx.writeAndFlush(response);
            }
        }
    }

    /**
     * Process response from remote peer to the previous issued requests.
     * <p>
     * 客户端响应给服务端的结果
     *
     * @param ctx channel handler context.
     * @param cmd response command instance.
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final int opaque = cmd.getOpaque();

        // 远端返回的响应，则需要根据 opaque 找到发起请求时候的 future
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture == null) {
            return;
        }

        // 设置客户端 cmd
        responseFuture.setResponseCommand(cmd);
        // 从映射表移除
        responseTable.remove(opaque);

        if (responseFuture.getInvokeCallback() != null) {
            // 回调 对象处理 结果
            executeInvokeCallback(responseFuture);
        } else {
            // 设置future的结果，这一步会调用 countDownLatch 的 countDown() 方法，唤醒业务线程
            responseFuture.putResponse(cmd);
            responseFuture.release();
        }
    }

    /**
     * Execute callback in callback executor. If callback executor is null, run directly in current thread
     *
     * @see ResponseFuture#getInvokeCallback() 执行这个对象的回调
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        // 是否在当前线程执行？
        boolean runInThisThread = false;

        // 拿到线程池
        ExecutorService executor = this.getCallbackExecutor();
        if (executor != null) {
            try {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // 回调
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            log.warn("execute callback in executor exception, and callback throw", e);
                        } finally {
                            responseFuture.release();
                        }
                    }
                });
            } catch (Exception e) {
                runInThisThread = true;
                log.warn("execute callback in executor exception, maybe executor busy", e);
            }
        } else {

            // 没有拿到线程池，则只能在当前线程执行了
            runInThisThread = true;
        }

        if (runInThisThread) {
            try {
                // 回调
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                log.warn("executeInvokeCallback Exception", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * Custom RPC hook.
     * Just be compatible with the previous version, use getRPCHooks instead.
     */
    @Deprecated
    protected RPCHook getRPCHook() {
        if (rpcHooks.size() > 0) {
            return rpcHooks.get(0);
        }
        return null;
    }

    /**
     * 定时任务会定时执行该方法，每秒钟执行一次，由{@link NettyRemotingServer#timer} 执行
     * <p>
     * This method is periodically invoked to scan and expire deprecated request.
     * 定期调用此方法来扫描和过期不推荐使用的请求。
     * </p>
     */
    public void scanResponseTable() {
        // 需要移除的队列
        final List<ResponseFuture> rfList = new LinkedList<ResponseFuture>();
        Iterator<Entry<Integer/* opaque */, ResponseFuture/*响应，里面封装了异步转同步的逻辑*/>> iterator = this.responseTable.entrySet().iterator();
        while (iterator.hasNext()) {
            // key:opaque,value:ResponseFuture
            Entry<Integer/* opaque */, ResponseFuture> next = iterator.next();
            ResponseFuture responseFuture = next.getValue();
            if ((responseFuture.getBeginTimestamp()/*开始请求的时间*/ + responseFuture.getTimeoutMillis()/*超时时间*/ + 1000) <= System.currentTimeMillis() /*超时的请求*/) {
                // 超时的需要定时清理,并发度-1
                responseFuture.release();
                // 从列表中移除
                iterator.remove();
                // 添加到超时列表中，后面会统一处理
                rfList.add(responseFuture);
            }
        }

        for (ResponseFuture rf : rfList) {
            try {
                // 超时的直接执行回调
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    /**
     * 服务器主动 主动向客户端发起请求的时候使用的方法 ---- 同步调用 服务器业务线程需要在这里等待Client返回结果之后整个调用才完毕
     *
     * 真正发送请求，网络连接的地方，发送消息的最底层方法了！！！！！！！！！！！
     *
     * @param channel ch
     * @param request 请求
     * @param timeoutMillis 超时时长
     * @return 请求？
     */
    public RemotingCommand invokeSyncImpl(final Channel channel/*客户端跟服务端的连接*/, final RemotingCommand request/*请求*/, final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        // 请求 id
        final int opaque = request.getOpaque();
        try {
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null, null);
            // 因为 netty 是一个异步的网络通信框架，怎么实现同步调用呢？答案就在这个映射表中！
            this.responseTable.put(opaque, responseFuture);

            // 写入请求，其实就是发起
            // 通过客户端channel写数据
            ChannelFuture channelFuture = channel.writeAndFlush(request);
            // 获取客户端地址信息
            final SocketAddress addr = channel.remoteAddress();
            // 注册监听器:channel线程回调
            channelFuture.addListener(new ChannelFutureListener() {
                // 提取注册监听器，注意：在操作完成之后再回调这个方法，并不是马上执行的！！！！！
                @Override
                public void operationComplete(ChannelFuture future) {
                    // 如果写成功，则
                    if (future.isSuccess()) {
                        /**
                         * 异步设置成true
                         * responseTable 中的数据在 finally 中移除
                         */
                        responseFuture.setSendRequestOK(true);
                        // 成功就直接返回了
                    } else {
                        // 失败的时候
                        responseFuture.setSendRequestOK(false);
                        // 失败的时候 也会执行，成功不会执行
                        responseTable.remove(opaque);
                        responseFuture.setCause(future.cause());
                        responseFuture.putResponse(null);
                    }
                }
            });
            /**
             * 服务器业务线程需要在这里等待Client返回结果之后整个调用才完毕
             * 业务线程在这里进入挂起状态
             * @see NettyRemotingAbstract#processResponseCommand(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)
             * @see ResponseFuture#putResponse(org.apache.rocketmq.remoting.protocol.RemotingCommand) 响应的时候调用这个方法唤醒
             */
            RemotingCommand responseCommand = responseFuture.waitResponse/* TODO 业务线程阻塞在这里*/(timeoutMillis);
            // 线程执行到这里
            // 1.正常情况：客户端返回数据了，IO线程将业务线程唤醒
            // 2.非正常全情况：超时了
            if (null == responseCommand) {
                // 进入这里：超时了。。。或者其他异常情况
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(addr), timeoutMillis, responseFuture.getCause());
                } else {
                    throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(addr), responseFuture.getCause());
                }
            }
            // 正常情况：成功，返回
            return responseCommand;
        } finally {
            this.responseTable.remove(opaque);
        }
    }

    /**
     * 服务器主动 主动向客户端发起请求的时候使用的方法
     *
     * @param channel 客户端ch
     * @param request 请求体
     * @param timeoutMillis 超时时长
     * @param invokeCallback 请求回调处理对象
     */
    @SneakyThrows
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback/*请求的对端返回的时候，会在调用端执行回调*/) {
        // 因为有超时的判断
        long beginStartTime = System.currentTimeMillis();
        // 请求id
        final int opaque = request.getOpaque();
        // 有并发度的限制，所以获取信号量
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            // 执行到这里，说明当前线程可以发起请求，服务器请求客户端的并发没有达到上限
            // 因为上面申请了一个信号量，所以封装释放信号量的逻辑的对象
            // 请求结果回来会释放以及超时的时候也会释放
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            // 记录到这里耗费的时间
            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeoutMillis < costTime) {
                // 已经超时了，直接走超时逻辑
                once.release();
                throw new RemotingTimeoutException("invokeAsyncImpl call timeout");
            }
            // 创建响应对象
            final ResponseFuture responseFuture = new ResponseFuture(channel/*客户端ch*/, opaque/*请求id*/, timeoutMillis - costTime/*剩余的超时时间*/, invokeCallback/*回调处理对象*/, once/*信号量释放对象*/);
            // 加入映射表中
            this.responseTable.put(opaque, responseFuture);
            try {
                // 1.业务线程将数据交给 netty，netty IO 线程接管写 和 刷 数据的操作
                // 2.注册 写刷 操作监听器，监听器由 IO 线程回调
                ChannelFuture writeAndFlushFuture = channel.writeAndFlush(request);

                // 上面的操作返回之后会回调这个监听器
                writeAndFlushFuture.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) {
                        if (f.isSuccess()) {
                            // 成功
                            responseFuture.setSendRequestOK(true);
                            return;
                        }
                        // 失败
                        requestFail(opaque);
                    }
                });
            } catch (Exception e) {
                responseFuture.release();
                log.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }

            // 异步调用就不会在这里阻塞了！！！！！
            // RemotingCommand responseCommand = responseFuture.waitResponse/*业务线程阻塞在这里*/(timeoutMillis);
        } else {
            // 获取信号量失败,并发高
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info = String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d", timeoutMillis, this.semaphoreAsync.getQueueLength(), this.semaphoreAsync.availablePermits());
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    // 请求发送完成即可，不关心结果
    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        // 在请求中做一个标记，告诉客户端不用返回结果
        request.markOnewayRPC();
        // 单向请求的并发控制
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            // 传递引用，响应拿到的时候需要是否信号量
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                ChannelFuture writeAndFlush = channel.writeAndFlush/*通过网络发送请求*/(request);
                writeAndFlush.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) {
                        // 操作完成，不管成功失败，都释放信号量
                        once.release();
                    }
                });
            } catch (Exception e) {
                once.release();
                log.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String msg = "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d";
                String info = String.format(msg, timeoutMillis, this.semaphoreOneway.getQueueLength(), this.semaphoreOneway.availablePermits());
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    /**
     * 请求失败
     *
     * @param opaque 请求id
     */
    private void requestFail(final int opaque) {
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                // 调用 回调处理器对象，异步 使用 CallbackExecutor(其实就是公共线程池)
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                log.warn("execute callback in requestFail, and callback throw", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * mark the request of the specified channel as fail and to invoke fail callback immediately
     *
     * @param channel the channel which is close already
     */
    protected void failFast(final Channel channel) {
        for (Entry<Integer/* opaque */, ResponseFuture> entry : responseTable.entrySet()) {
            ResponseFuture responseFuture = entry.getValue();
            Channel processChannel = responseFuture.getProcessChannel();
            if (processChannel == channel) {
                Integer opaque = entry.getKey();
                if (opaque != null) {
                    requestFail(opaque);
                }
            }
        }
    }

    /**
     * @see org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService namesrv使用
     * @see org.apache.rocketmq.broker.client.ClientHousekeepingService broker 使用,监听客户端的连接状态
     */
    class NettyEventExecutor extends ServiceThread {

        /**
         * 子类的成员变量
         * 任务队列
         */
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();

        public void putNettyEvent(final NettyEvent event) {
            int maxSize = 10000;
            if (this.eventQueue.size() <= maxSize) {
                this.eventQueue.add(event);
            } else {
                log.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(), event.toString());
            }
        }

        @Override
        public void run() {
            /**
             * @see org.apache.rocketmq.broker.client.ClientHousekeepingService broker
             * @see org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService namesrv
             */
            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();
            while (!this.isStopped()) {
                try {
                    // 拿到一个 netty 事件！！！
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }
        }
    }

    //************* 下面为抽象方法 *********//

    /**
     * Custom channel event listener.
     *
     * @return custom channel event listener if defined; null otherwise.
     * @see org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService
     * @see org.apache.rocketmq.broker.client.ClientHousekeepingService
     */
    public abstract ChannelEventListener getChannelEventListener();

    /**
     * This method specifies thread pool to use while invoking callback methods.
     *
     * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
     * netty event-loop thread.
     */
    public abstract ExecutorService getCallbackExecutor();
}

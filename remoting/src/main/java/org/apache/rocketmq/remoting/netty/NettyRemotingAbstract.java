package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import lombok.Getter;
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

public abstract class NettyRemotingAbstract {

    /**
     * Remoting logger instance.
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * Semaphore to limit maximum number of on-going one-way requests, which protects system memory footprint.
     */
    protected final Semaphore semaphoreOneway;

    /**
     * 异步调用信号量
     * Semaphore to limit maximum number of on-going asynchronous requests, which protects system memory footprint.
     */
    protected final Semaphore semaphoreAsync;

    /**
     * This map caches all on-going requests.
     * 请求响应映射表
     *
     * @see RemotingCommand#requestId
     */
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable = new ConcurrentHashMap<Integer, ResponseFuture>(256);

    /**
     * This container holds all processors per request code, aka, for each incoming request, we may look up the
     * responding processor in this map to handle the request.
     */
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable = new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);

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
     * Custom channel event listener.
     *
     * @return custom channel event listener if defined; null otherwise.
     */
    public abstract ChannelEventListener getChannelEventListener();

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
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        // 如果没找到，则使用默认的
        final Pair<NettyRequestProcessor, ExecutorService> pair = (null == matched) ? this.defaultRequestProcessor : matched;
        // 拿到请求id
        final int opaque = cmd.getOpaque();
        if (pair == null) {
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setOpaque(opaque);
            ctx.writeAndFlush(response);
            log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
            return;
        }
        // 核心逻辑 start
        Runnable run = new Runnable() {
            @Override
            public void run() {
                try {
                    // 执行rpc调用前的钩子函数
                    doBeforeRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) /* 通过channel解析出远程的地址 */, cmd);

                    // 如果是异步的，任务执行完成之后会回调到该对象的 callback 函数
                    final RemotingResponseCallback callback = new RemotingResponseCallback() {
                        @Override
                        public void callback(RemotingCommand response) {
                            // 执行rpc调用后的钩子函数
                            doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel())/* 通过channel解析出远程的地址 */, cmd, response);
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
                                    log.error("process request over, but response failed", e);
                                    log.error(cmd.toString());
                                    log.error(response.toString());
                                }
                            }
                        }
                    };

                    if (pair.getObject1() instanceof AsyncNettyRequestProcessor) {
                        // namesrv 进入这里
                        AsyncNettyRequestProcessor processor = (AsyncNettyRequestProcessor) pair.getObject1();
                        // 执行任务，不返回响应，而是通过异步的方式
                        processor.asyncProcessRequest(ctx, cmd, callback);
                    } else {
                        // 如果是同步的，则执行这个分支
                        NettyRequestProcessor processor = pair.getObject1();
                        // 执行任务，拿到响应
                        RemotingCommand response = processor.processRequest(ctx, cmd);
                        // 执行rpc调用后的钩子函数
                        doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel())/* 通过channel解析出远程的地址 */, cmd, response);
                        // 同步的任务，当前线程自己主动调用 callback，把结果写到对端
                        callback.callback(response);
                    }
                } catch (Throwable e) {
                    log.error("process request exception", e);
                    log.error(cmd.toString());
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
        if (pair.getObject1().rejectRequest()) {
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
        } catch (RejectedExecutionException e) {
            if ((System.currentTimeMillis() % 10000) == 0) {
                log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + ", too many requests and system thread pool busy, RejectedExecutionException " + pair.getObject2().toString() + " request code: " + cmd.getCode());
            }
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
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture == null) {
            log.warn("receive response, but not matched any request, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            log.warn(cmd.toString());
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
     * This method specifies thread pool to use while invoking callback methods.
     *
     * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
     * netty event-loop thread.
     */
    public abstract ExecutorService getCallbackExecutor();

    /**
     * <p>
     * This method is periodically invoked to scan and expire deprecated request.
     * </p>
     */
    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<ResponseFuture>();
        // ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable
        // key:opaque,value:ResponseFuture
        // ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable
        Iterator<Entry<Integer/* opaque */, ResponseFuture>> iterator = this.responseTable.entrySet().iterator();
        while (iterator.hasNext()) {
            // key:opaque,value:ResponseFuture
            Entry<Integer/* opaque */, ResponseFuture> next = iterator.next();
            ResponseFuture responseFuture = next.getValue();
            if ((responseFuture.getBeginTimestamp() + responseFuture.getTimeoutMillis() + 1000) <= System.currentTimeMillis() /*超时的请求*/) {
                // 超时的需要定时清理
                responseFuture.release();
                // 从列表中移除
                iterator.remove();

                // 添加到超时列表中，后面会统一处理
                rfList.add(responseFuture);
                log.warn("remove timeout request, " + responseFuture);
            }
        }

        for (ResponseFuture rf : rfList) {
            try {
                // TODO 超时的直接执行回调？？为什么呢？？
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
    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        // 请求 id
        final int opaque = request.getOpaque();
        try {
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null, null);
            // 先不管那么多，先放到映射表
            // key:opaque
            this.responseTable.put(opaque, responseFuture);
            // 获取客户端地址信息
            final SocketAddress addr = channel.remoteAddress();
            // 写入请求，其实就是发起
            // 通过客户端channel写数据
            ChannelFuture channelFuture = channel.writeAndFlush(request);
            // 注册监听器:channel线程回调
            channelFuture.addListener(new ChannelFutureListener() {
                // 提取注册监听器，注意：在操作完成之后再回调这个方法，并不是马上执行的！！！！！

                @Override
                public void operationComplete(ChannelFuture future) {
                    // 如果写成功，则
                    if (future.isSuccess()) {
                        // 异步设置成true
                        responseFuture.setSendRequestOK(true);
                        // 成功就直接返回了
                    } else {
                        // 失败的时候
                        responseFuture.setSendRequestOK(false);
                        // 失败的时候 也会执行，成功不会执行
                        responseTable.remove(opaque);
                        responseFuture.setCause(future.cause());
                        responseFuture.putResponse(null);
                        log.warn("send a request command to channel <" + addr + "> failed.");
                    }
                }
            });
            // 服务器业务线程需要在这里等待Client返回结果之后整个调用才完毕
            // 业务线程在这里进入挂起状态
            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
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
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
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
            final ResponseFuture responseFuture = new ResponseFuture(channel,/*客户端ch*/opaque,/*请求id*/timeoutMillis - costTime,/*剩余的超时时间*/invokeCallback,/*回调处理对象*/once/*信号量释放对象*/);
            // 加入映射表中
            this.responseTable.put(opaque, responseFuture);
            try {
                // 1.业务线程将数据交给 netty，netty IO 线程接管写 和 刷 数据的操作
                // 2.注册 写刷 操作监听器，监听器由 IO 线程回调
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) {
                        if (f.isSuccess()) {
                            // 成功
                            responseFuture.setSendRequestOK(true);
                            return;
                        }
                        // 失败
                        requestFail(opaque);
                        log.warn("send a request command to channel <{}> failed.", RemotingHelper.parseChannelRemoteAddr(channel));
                    }
                });
            } catch (Exception e) {
                responseFuture.release();
                log.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
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
        // ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable
        for (Entry<Integer, ResponseFuture> entry : responseTable.entrySet()) {
            if (entry.getValue().getProcessChannel() == channel) {
                Integer opaque = entry.getKey();
                if (opaque != null) {
                    requestFail(opaque);
                }
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
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) {
                        // 操作完成，不管成功失败，都释放信号量
                        once.release();
                        if (!f.isSuccess()) {
                            // 失败打印日志
                            log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                        }
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
            log.info(this.getServiceName() + " service started");
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

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }
}

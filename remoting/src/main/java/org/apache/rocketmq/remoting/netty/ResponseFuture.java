package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@ToString
public class ResponseFuture {

    /**
     * 发送请求的时候生成的唯一编号
     */
    @Getter
    private final int opaque;

    /**
     * 发送请求的时候与服务端建立的连接
     */
    @Getter
    private final Channel processChannel;

    @Getter
    private final long timeoutMillis;

    /**
     * 如果该字段不为空，则在响应的时候回调该对象
     *
     * @see NettyRemotingAbstract#processResponseCommand(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)
     */
    @Getter
    private final InvokeCallback invokeCallback;

    @Getter
    private final long beginTimestamp = System.currentTimeMillis();

    /**
     * 在这个对象上等待服务器响应
     *
     * 发起请求的线程在这个对象等待
     */
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    private final SemaphoreReleaseOnlyOnce once;

    /**
     * 保证每个响应只执行一次回调
     */
    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);

    /**
     * 该请求收到的响应
     */
    @Getter
    @Setter
    private volatile RemotingCommand responseCommand;

    @Getter
    @Setter
    private volatile boolean sendRequestOK = true;

    // 失败原因
    @Getter
    @Setter
    private volatile Throwable cause;

    /**
     * @param channel ch
     * @param timeoutMillis 超时时长
     * @param invokeCallback 请求回调处理对象
     * @param opaque 请求Id
     * @param once 封装释放信号量的逻辑
     * @see NettyRemotingAbstract#invokeAsyncImpl(io.netty.channel.Channel, org.apache.rocketmq.remoting.protocol.RemotingCommand, long, org.apache.rocketmq.remoting.InvokeCallback)
     */
    public ResponseFuture(Channel channel, int opaque, long timeoutMillis, InvokeCallback invokeCallback, SemaphoreReleaseOnlyOnce once) {
        this.opaque = opaque;
        this.processChannel = channel;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }

    public void executeInvokeCallback() {
        if (invokeCallback != null) {
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                invokeCallback.operationComplete(this);
            }
        }
    }

    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    @SuppressWarnings("all")
    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        // 线程挂起原理！业务线程阻塞在这里
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    public void putResponse(final RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
    }
}

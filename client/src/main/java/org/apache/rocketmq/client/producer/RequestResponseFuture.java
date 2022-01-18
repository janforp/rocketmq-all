package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RequestResponseFuture {

    // 相关标识,uuid
    @Getter
    private final String correlationId;

    /**
     * * public interface RequestCallback {
     * *
     * *     void onSuccess(final Message message);
     * *
     * *     void onException(final Throwable e);
     * * }
     */
    @Getter
    private final RequestCallback requestCallback;

    @Getter
    private final long beginTimestamp = System.currentTimeMillis();

    @Getter
    private final Message requestMsg = null;

    // 超时时间
    @Getter
    @Setter
    private long timeoutMillis;

    @Getter
    @Setter
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Getter
    @Setter
    private volatile Message responseMsg = null;

    // 发送请求是否成功
    @Getter
    @Setter
    private volatile boolean sendRequestOk = true;

    @Getter
    @Setter
    private volatile Throwable cause = null;

    public RequestResponseFuture(String correlationId, long timeoutMillis, RequestCallback requestCallback) {
        this.correlationId = correlationId;
        this.timeoutMillis = timeoutMillis;
        this.requestCallback = requestCallback;
    }

    public void executeRequestCallback() {
        if (requestCallback != null) {
            if (sendRequestOk && cause == null) {
                requestCallback.onSuccess(responseMsg);
            } else {
                requestCallback.onException(cause);
            }
        }
    }

    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    // 等待结果，阻塞
    public Message waitResponseMessage(final long timeout) throws InterruptedException {
        boolean await = this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        System.out.println(await);
        return this.responseMsg;
    }

    // 设置结果，唤醒
    public void putResponseMessage(final Message responseMsg) {
        this.responseMsg = responseMsg;
        this.countDownLatch.countDown();
    }
}

package org.apache.rocketmq.client.producer;

import lombok.Getter;
import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * request方法发送的消息需要 消费者回执一条消息
 * 怎么实现的呢？
 * 生产者msg加了一些信息，关联ID客户端ID，发送到broker之后，消费者从 broker 拿到这条消息，检查msg,发现是一个需要回执的消息
 * 处理完消息之后，根据msg关联的ID以及客户端Id生成一条消息（封装响应给生产者的结果）发送到broker。
 * broker拿到这条消息之后，它知道这是一条回执消息，根据客户端Id找到ch，将消息推送给生产者，
 * 生产者这边拿到回执消息之后，读取出来关联ID，找到对应的 RequestFuture，将阻塞的线程唤醒
 *
 * 类似 生产者 和 消费者 之间进行了一次 RPC，只不过 中间由 broker 代理完成！
 */
public class RequestFutureTable {

    private static final InternalLogger log = ClientLogger.getLog();

    @Getter
    private static final ConcurrentHashMap<String/*correlationId：uuid*/, RequestResponseFuture> requestFutureTable = new ConcurrentHashMap<String, RequestResponseFuture>();

    public static void scanExpiredRequest() {
        final List<RequestResponseFuture> rfList = new LinkedList<RequestResponseFuture>();
        Iterator<Map.Entry<String, RequestResponseFuture>> it = requestFutureTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, RequestResponseFuture> next = it.next();
            RequestResponseFuture rep = next.getValue();

            if (rep.isTimeout()) {
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, CorrelationId={}" + rep.getCorrelationId());
            }
        }

        for (RequestResponseFuture rf : rfList) {
            try {
                Throwable cause = new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, "request timeout, no reply message.");
                rf.setCause(cause);
                rf.executeRequestCallback();
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }
}
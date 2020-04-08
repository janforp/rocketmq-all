package org.apache.rocketmq.client.trace;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;

import java.io.IOException;

/**
 * Interface of asynchronous transfer data
 */
public interface TraceDispatcher {

    enum Type {
        PRODUCE,
        CONSUME
    }

    /**
     * Initialize asynchronous transfer data module
     */
    void start(String nameSrvAddr, AccessChannel accessChannel) throws MQClientException;

    /**
     * Append the transfering data
     *
     * @param ctx data infomation
     * @return
     */
    boolean append(Object ctx);

    /**
     * Write flush action
     *
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Close the trace Hook
     */
    void shutdown();
}

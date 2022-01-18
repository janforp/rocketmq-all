package org.apache.rocketmq.client.hook;

import org.apache.rocketmq.client.exception.MQClientException;

public interface CheckForbiddenHook {

    String hookName();

    /**
     * 可以发送异常的哦
     */
    void checkForbidden(final CheckForbiddenContext context) throws MQClientException;
}

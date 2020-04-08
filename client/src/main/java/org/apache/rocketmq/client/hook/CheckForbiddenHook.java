package org.apache.rocketmq.client.hook;

import org.apache.rocketmq.client.exception.MQClientException;

public interface CheckForbiddenHook {

    String hookName();

    void checkForbidden(final CheckForbiddenContext context) throws MQClientException;
}

package org.apache.rocketmq.client.hook;

public interface FilterMessageHook {

    String hookName();

    void filterMessage(final FilterMessageContext context);
}
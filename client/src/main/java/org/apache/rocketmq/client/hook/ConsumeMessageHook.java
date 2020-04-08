package org.apache.rocketmq.client.hook;

public interface ConsumeMessageHook {

    String hookName();

    void consumeMessageBefore(final ConsumeMessageContext context);

    void consumeMessageAfter(final ConsumeMessageContext context);
}

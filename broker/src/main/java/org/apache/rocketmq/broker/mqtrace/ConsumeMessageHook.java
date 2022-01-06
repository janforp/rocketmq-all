package org.apache.rocketmq.broker.mqtrace;

public interface ConsumeMessageHook {

    String hookName();

    void consumeMessageBefore(final ConsumeMessageContext context);

    void consumeMessageAfter(final ConsumeMessageContext context);
}
package org.apache.rocketmq.client.hook;

public interface SendMessageHook {

    String hookName();

    void sendMessageBefore(final SendMessageContext context);

    void sendMessageAfter(final SendMessageContext context);
}

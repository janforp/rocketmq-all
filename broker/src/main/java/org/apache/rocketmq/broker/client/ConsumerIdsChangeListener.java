package org.apache.rocketmq.broker.client;

public interface ConsumerIdsChangeListener {

    void handle(ConsumerGroupEvent event, String group, Object... args);
}
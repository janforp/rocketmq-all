package org.apache.rocketmq.broker.client;

public enum ConsumerGroupEvent {

    /**
     * Some consumers in the group are changed.
     */
    CHANGE,
    /**
     * The group of consumer is unregistered.
     */
    UNREGISTER,
    /**
     * The group of consumer is registered.
     */
    REGISTER
}
package org.apache.rocketmq.store;

/**
 * Used when trying to put message
 */
public interface PutMessageLock {

    void lock();

    void unlock();
}
package org.apache.rocketmq.store;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Exclusive lock implementation to put message
 */
public class PutMessageReentrantLock implements PutMessageLock {

    private final ReentrantLock putMessageNormalLock = new ReentrantLock(); // NonfairSync

    @Override
    public void lock() {
        putMessageNormalLock.lock();
    }

    @Override
    public void unlock() {
        putMessageNormalLock.unlock();
    }
}

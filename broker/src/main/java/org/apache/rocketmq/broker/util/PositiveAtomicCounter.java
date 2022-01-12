package org.apache.rocketmq.broker.util;

import java.util.concurrent.atomic.AtomicInteger;

public class PositiveAtomicCounter {

    private static final int MASK = 0x7FFFFFFF;

    private final AtomicInteger atom;

    public PositiveAtomicCounter() {
        atom = new AtomicInteger(0);
    }

    public final int incrementAndGet() {
        final int rt = atom.incrementAndGet();
        return rt & MASK;
    }

    public int intValue() {
        return atom.intValue();
    }
}

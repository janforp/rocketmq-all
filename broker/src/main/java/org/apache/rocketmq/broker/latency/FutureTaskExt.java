package org.apache.rocketmq.broker.latency;

import lombok.Getter;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class FutureTaskExt<V> extends FutureTask<V> {

    @Getter
    private final Runnable runnable;

    public FutureTaskExt(final Callable<V> callable) {
        super(callable);
        this.runnable = null;
    }

    public FutureTaskExt(final Runnable runnable, final V result) {
        super(runnable, result);
        this.runnable = runnable;
    }
}

package org.apache.rocketmq.common;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.atomic.AtomicLong;

@ToString
public class DataVersion extends RemotingSerializable {

    @Getter
    @Setter
    private long timestamp = System.currentTimeMillis();

    @Getter
    @Setter
    private AtomicLong counter = new AtomicLong(0);

    public void assignNewOne(final DataVersion dataVersion) {
        this.timestamp = dataVersion.timestamp;
        this.counter.set(dataVersion.counter.get());
    }

    public void nextVersion() {
        this.timestamp = System.currentTimeMillis();
        this.counter.incrementAndGet();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final DataVersion that = (DataVersion) o;

        if (timestamp != that.timestamp) {
            return false;
        }

        if (counter != null && that.counter != null) {
            return counter.longValue() == that.counter.longValue();
        }

        return (null == counter) && (null == that.counter);
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        if (null != counter) {
            long l = counter.get();
            result = 31 * result + (int) (l ^ (l >>> 32));
        }
        return result;
    }
}

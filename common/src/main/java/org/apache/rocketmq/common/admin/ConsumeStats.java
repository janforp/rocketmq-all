package org.apache.rocketmq.common.admin;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

public class ConsumeStats extends RemotingSerializable {

    @Setter
    @Getter
    private HashMap<MessageQueue, OffsetWrapper> offsetTable = new HashMap<MessageQueue, OffsetWrapper>();

    @Setter
    @Getter
    private double consumeTps = 0;

    public long computeTotalDiff() {
        long diffTotal = 0L;

        Iterator<Entry<MessageQueue, OffsetWrapper>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, OffsetWrapper> next = it.next();
            long diff = next.getValue().getBrokerOffset() - next.getValue().getConsumerOffset();
            diffTotal += diff;
        }

        return diffTotal;
    }
}

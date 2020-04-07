package org.apache.rocketmq.common.protocol.body;

import lombok.Data;

@Data
public class ConsumeQueueData {

    private long physicOffset;

    private int physicSize;

    private long tagsCode;

    private String extendDataJson;

    private String bitMap;

    private boolean eval;

    private String msg;

    @Override
    public String toString() {
        return "ConsumeQueueData{" +
                "physicOffset=" + physicOffset +
                ", physicSize=" + physicSize +
                ", tagsCode=" + tagsCode +
                ", extendDataJson='" + extendDataJson + '\'' +
                ", bitMap='" + bitMap + '\'' +
                ", eval=" + eval +
                ", msg='" + msg + '\'' +
                '}';
    }
}

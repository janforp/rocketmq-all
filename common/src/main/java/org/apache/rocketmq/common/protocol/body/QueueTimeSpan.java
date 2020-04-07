package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Date;

@Getter
@Setter
public class QueueTimeSpan {

    private MessageQueue messageQueue;

    private long minTimeStamp;

    private long maxTimeStamp;

    private long consumeTimeStamp;

    private long delayTime;


    public String getMinTimeStampStr() {
        return UtilAll.formatDate(new Date(minTimeStamp), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);
    }

    public String getMaxTimeStampStr() {
        return UtilAll.formatDate(new Date(maxTimeStamp), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);
    }

    public String getConsumeTimeStampStr() {
        return UtilAll.formatDate(new Date(consumeTimeStamp), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);
    }
}

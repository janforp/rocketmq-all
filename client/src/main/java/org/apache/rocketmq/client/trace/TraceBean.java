package org.apache.rocketmq.client.trace;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageType;

@Getter
@Setter
public class TraceBean {

    private static final String LOCAL_ADDRESS = UtilAll.ipToIPv4Str(UtilAll.getIP());

    private String topic = "";

    private String msgId = "";

    private String offsetMsgId = "";

    private String tags = "";

    private String keys = "";

    private String storeHost = LOCAL_ADDRESS;

    private String clientHost = LOCAL_ADDRESS;

    private long storeTime;

    private int retryTimes;

    private int bodyLength;

    private MessageType msgType;
}

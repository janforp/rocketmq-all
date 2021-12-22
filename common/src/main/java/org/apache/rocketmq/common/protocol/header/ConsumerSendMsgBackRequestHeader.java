package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@ToString
public class ConsumerSendMsgBackRequestHeader implements CommandCustomHeader {

    @CFNotNull
    @Getter
    @Setter
    private Long offset;

    @CFNotNull
    @Getter
    @Setter
    private String group;

    @CFNotNull
    @Getter
    @Setter
    private Integer delayLevel;

    @Getter
    @Setter
    private String originMsgId;

    @Getter
    @Setter
    private String originTopic;

    @CFNullable
    @Getter
    @Setter
    private boolean unitMode = false;

    @Getter
    @Setter
    private Integer maxReconsumeTimes;

    @Override
    public void checkFields() throws RemotingCommandException {

    }
}
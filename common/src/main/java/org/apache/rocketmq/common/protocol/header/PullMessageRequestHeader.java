package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Getter
@Setter
public class PullMessageRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String consumerGroup;

    @CFNotNull
    private String topic;

    @CFNotNull
    private Integer queueId;

    @CFNotNull
    private Long queueOffset;

    @CFNotNull
    private Integer maxMsgNums;

    @CFNotNull
    private Integer sysFlag;

    @CFNotNull
    private Long commitOffset;

    @CFNotNull
    private Long suspendTimeoutMillis;

    @CFNullable
    private String subscription;

    @CFNotNull
    private Long subVersion;

    private String expressionType;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
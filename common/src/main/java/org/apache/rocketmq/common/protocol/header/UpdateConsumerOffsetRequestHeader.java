package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Getter
@Setter
public class UpdateConsumerOffsetRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String consumerGroup;

    @CFNotNull
    private String topic;

    @CFNotNull
    private Integer queueId;

    @CFNotNull
    private Long commitOffset;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
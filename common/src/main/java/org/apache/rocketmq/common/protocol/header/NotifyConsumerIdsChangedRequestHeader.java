package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class NotifyConsumerIdsChangedRequestHeader implements CommandCustomHeader {

    @Getter
    @Setter
    @CFNotNull
    private String consumerGroup;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
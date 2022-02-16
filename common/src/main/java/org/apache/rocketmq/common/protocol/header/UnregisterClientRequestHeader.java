package org.apache.rocketmq.common.protocol.header;

import lombok.Data;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Data
public class UnregisterClientRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String clientID;

    @CFNullable
    private String producerGroup;

    @CFNullable
    private String consumerGroup;

    @Override
    public void checkFields() throws RemotingCommandException {

    }
}
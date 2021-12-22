package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class QueryConsumerOffsetResponseHeader implements CommandCustomHeader {

    @CFNotNull
    @Setter
    @Getter
    private Long offset;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
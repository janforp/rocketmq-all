package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetConsumerListByGroupResponseHeader implements CommandCustomHeader {

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
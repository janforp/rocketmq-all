package org.apache.rocketmq.common.protocol.header.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetRouteInfoRequestHeader implements CommandCustomHeader {

    @CFNotNull
    @Getter
    @Setter
    private String topic;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
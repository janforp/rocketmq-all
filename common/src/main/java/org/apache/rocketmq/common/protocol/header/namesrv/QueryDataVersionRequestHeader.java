package org.apache.rocketmq.common.protocol.header.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Getter
@Setter
public class QueryDataVersionRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String brokerName;

    @CFNotNull
    private String brokerAddr;

    @CFNotNull
    private String clusterName;

    @CFNotNull
    private Long brokerId;

    @Override
    public void checkFields() throws RemotingCommandException {

    }
}
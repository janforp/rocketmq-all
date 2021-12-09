package org.apache.rocketmq.common.protocol.header.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Getter
@Setter
public class RegisterBrokerResponseHeader implements CommandCustomHeader {

    @CFNullable
    private String haServerAddr;

    @CFNullable
    private String masterAddr;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
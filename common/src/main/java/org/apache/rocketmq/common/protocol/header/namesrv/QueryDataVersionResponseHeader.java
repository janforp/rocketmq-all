package org.apache.rocketmq.common.protocol.header.namesrv;

import lombok.Data;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Data
public class QueryDataVersionResponseHeader implements CommandCustomHeader {

    @CFNotNull
    private Boolean changed;

    @Override
    public void checkFields() throws RemotingCommandException {

    }
}
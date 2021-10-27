package org.apache.rocketmq.common.protocol.header.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Setter
@Getter
public class PutKVConfigRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String namespace;

    @CFNotNull
    private String key;

    @CFNotNull
    private String value;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Getter
@Setter
public class GetConsumerStatusRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String topic;

    @CFNotNull
    private String group;

    @CFNullable
    private String clientAddr;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}

package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Setter
@Getter
public class ResetOffsetRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String topic;

    @CFNotNull
    private String group;

    @CFNotNull
    private long timestamp;

    @CFNotNull
    private boolean isForce;

    @Override
    public void checkFields() throws RemotingCommandException {

    }
}

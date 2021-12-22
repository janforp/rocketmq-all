package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Setter
@Getter
public class QueryMessageResponseHeader implements CommandCustomHeader {

    @CFNotNull
    private Long indexLastUpdateTimestamp;

    @CFNotNull
    private Long indexLastUpdatePhyoffset;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}

package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Setter
@Getter
public class PullMessageResponseHeader implements CommandCustomHeader {

    @CFNotNull
    private Long suggestWhichBrokerId;

    @CFNotNull
    private Long nextBeginOffset;

    @CFNotNull
    private Long minOffset;

    @CFNotNull
    private Long maxOffset;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}

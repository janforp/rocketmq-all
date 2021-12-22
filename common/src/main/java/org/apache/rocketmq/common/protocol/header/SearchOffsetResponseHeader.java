package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Getter
@Setter
public class SearchOffsetResponseHeader implements CommandCustomHeader {

    @CFNotNull
    private Long offset;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}

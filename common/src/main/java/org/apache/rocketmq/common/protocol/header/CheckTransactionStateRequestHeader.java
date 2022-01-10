package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Getter
@Setter
public class CheckTransactionStateRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private Long tranStateTableOffset;

    @CFNotNull
    private Long commitLogOffset;

    private String msgId;

    private String transactionId;

    private String offsetMsgId;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
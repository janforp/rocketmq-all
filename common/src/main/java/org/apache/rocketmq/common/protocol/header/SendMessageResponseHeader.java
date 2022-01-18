package org.apache.rocketmq.common.protocol.header;

import lombok.Data;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Data
public class SendMessageResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private String msgId;
    @CFNotNull
    private Integer queueId;
    @CFNotNull
    private Long queueOffset;
    private String transactionId;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}

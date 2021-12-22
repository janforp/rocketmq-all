package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Getter
@Setter
public class QueryMessageRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String topic;

    @CFNotNull
    private String key;

    @CFNotNull
    private Integer maxNum;

    @CFNotNull
    private Long beginTimestamp;

    @CFNotNull
    private Long endTimestamp;

    @Override
    public void checkFields() throws RemotingCommandException {

    }
}
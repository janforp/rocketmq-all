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

    // 返回给客户端，表示下一次再向当前队列拉消息的时候的开始 offset
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

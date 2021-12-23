package org.apache.rocketmq.common.protocol.header;

import java.util.List;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

@Setter
@Getter
public class GetConsumerListByGroupResponseBody extends RemotingSerializable {

    private List<String> consumerIdList;
}

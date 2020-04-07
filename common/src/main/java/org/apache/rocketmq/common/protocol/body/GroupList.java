package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;

public class GroupList extends RemotingSerializable {

    @Getter
    @Setter
    private HashSet<String> groupList = new HashSet<String>();
}

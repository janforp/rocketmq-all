package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class BrokerStatsData extends RemotingSerializable {

    @Setter
    @Getter
    private BrokerStatsItem statsMinute;

    @Setter
    @Getter
    private BrokerStatsItem statsHour;

    @Setter
    @Getter
    private BrokerStatsItem statsDay;
}

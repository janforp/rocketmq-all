package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class ClusterAclVersionInfo extends RemotingSerializable {

    @Getter
    @Setter
    private String brokerName;

    @Getter
    @Setter
    private String brokerAddr;

    @Getter
    @Setter
    private DataVersion aclConfigDataVersion;

    @Getter
    @Setter
    private String clusterName;
}

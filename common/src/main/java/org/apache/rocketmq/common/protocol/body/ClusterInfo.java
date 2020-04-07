package org.apache.rocketmq.common.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class ClusterInfo extends RemotingSerializable {

    @Setter
    @Getter
    private HashMap<String/* brokerName */, BrokerData> brokerAddrTable;

    @Setter
    @Getter
    private HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;

    public String[] retrieveAllAddrByCluster(String cluster) {
        List<String> addrs = new ArrayList<String>();
        if (clusterAddrTable.containsKey(cluster)) {
            Set<String> brokerNames = clusterAddrTable.get(cluster);
            for (String brokerName : brokerNames) {
                BrokerData brokerData = brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    addrs.addAll(brokerData.getBrokerAddrs().values());
                }
            }
        }

        return addrs.toArray(new String[] {});
    }

    public String[] retrieveAllClusterNames() {
        return clusterAddrTable.keySet().toArray(new String[] {});
    }
}

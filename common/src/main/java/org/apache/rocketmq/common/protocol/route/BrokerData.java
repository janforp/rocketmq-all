package org.apache.rocketmq.common.protocol.route;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.common.MixAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * broker 信息
 */
@ToString
@NoArgsConstructor
public class BrokerData implements Comparable<BrokerData> {

    /**
     * 集群名称
     */
    @Getter
    @Setter
    private String cluster;

    /**
     * 节点名称
     */
    @Getter
    @Setter
    private String brokerName;

    /**
     * 地址
     * key:brokerId值为0的机器节点为master,其他为slave
     * value:服务IP地址，列：192.168.0.1:210000
     */
    @Getter
    @Setter
    private HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs;

    private final Random random = new Random();

    @Override
    public int compareTo(BrokerData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
    }

    /**
     * Selects a (preferably master) broker address from the registered list.
     * If the master's address cannot be found, a slave broker address is selected in a random manner.
     *
     * @return Broker address.
     */
    public String selectBrokerAddr() {
        String addr = this.brokerAddrs.get(MixAll.MASTER_ID);

        if (addr == null) {
            List<String> addrs = new ArrayList<String>(brokerAddrs.values());
            return addrs.get(random.nextInt(addrs.size()));
        }

        return addr;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerAddrs == null) ? 0 : brokerAddrs.hashCode());
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BrokerData other = (BrokerData) obj;
        if (brokerAddrs == null) {
            if (other.brokerAddrs != null) {
                return false;
            }
        } else if (!brokerAddrs.equals(other.brokerAddrs)) {
            return false;
        }
        if (brokerName == null) {
            if (other.brokerName != null) {
                return false;
            }
        } else if (!brokerName.equals(other.brokerName)) {
            return false;
        }
        return true;
    }
}

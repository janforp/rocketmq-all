package org.apache.rocketmq.common.protocol.route;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.common.MixAll;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * 一个 brokerName 逻辑节点下面可以有多个 物理节点！！！！
 * broker 信息,包括主从 broker，只要 brokerName 相关的 broker 都会封装在一个对象中，其实都是有配置决定
 *
 * @see broker.properties
 * #broker名字，注意此处不同的配置文件填写的不一样
 * brokerName=broker-a
 * #0 表示 Master，>0 表示 Slave
 * brokerId=0
 */
@ToString
@NoArgsConstructor
public class BrokerData implements Comparable<BrokerData> {

    /**
     * 集群名称，brokerClusterName=rocketmq-cluster
     *
     * 同一个集群下面会存在多个 brokerName,同一个 brokerName 可能有多台 broker 服务，包括主从
     */
    @Getter
    @Setter
    private String cluster;

    /**
     * 节点名称，如：broker-a
     */
    @Getter
    @Setter
    private String brokerName;

    /**
     * 地址
     * key:brokerId值为0的机器节点为master,其他为slave
     * value:服务IP地址，列：192.168.0.1:210000
     *
     * #broker名字，注意此处不同的配置文件填写的不一样
     * brokerName=broker-a
     * #0 表示 Master，>0 表示 Slave
     * brokerId=0
     */
    @Getter
    @Setter
    private HashMap<Long/* brokerId,如 0为master，其他为 slave节点  */, String/* broker address 如 127.0.0.1:10911*/> brokerAddrs;

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

        // master 优先，如果没有则用 slave
        if (addr == null) {
            // 如 127.0.0.1:10911
            Collection<String> brokerAddress = brokerAddrs.values();
            List<String> addrs = new ArrayList<String>(brokerAddress);
            // 随机
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
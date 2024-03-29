package org.apache.rocketmq.common.protocol.route;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@ToString
public class TopicRouteData extends RemotingSerializable {

    // if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) 成立则该主题为顺序消息
    //             String[] brokers = route.getOrderTopicConf().split(";");

    /**
     * @see org.apache.rocketmq.client.impl.factory.MQClientInstance#topicRouteData2TopicPublishInfo(java.lang.String, org.apache.rocketmq.common.protocol.route.TopicRouteData)
     */
    @Setter
    @Getter
    private String orderTopicConf;

    /**
     * 队列信息
     */
    @Setter
    @Getter
    private List<QueueData> queueDatas;

    /**
     * broker 节点信息
     */
    @Setter
    @Getter
    private List<BrokerData> brokerDatas;

    @Setter
    @Getter
    private HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    public TopicRouteData cloneTopicRouteData() {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setQueueDatas(new ArrayList<QueueData>());
        topicRouteData.setBrokerDatas(new ArrayList<BrokerData>());
        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        topicRouteData.setOrderTopicConf(this.orderTopicConf);

        if (this.queueDatas != null) {
            topicRouteData.getQueueDatas().addAll(this.queueDatas);
        }

        if (this.brokerDatas != null) {
            topicRouteData.getBrokerDatas().addAll(this.brokerDatas);
        }

        if (this.filterServerTable != null) {
            topicRouteData.getFilterServerTable().putAll(this.filterServerTable);
        }
        return topicRouteData;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerDatas == null) ? 0 : brokerDatas.hashCode());
        result = prime * result + ((orderTopicConf == null) ? 0 : orderTopicConf.hashCode());
        result = prime * result + ((queueDatas == null) ? 0 : queueDatas.hashCode());
        result = prime * result + ((filterServerTable == null) ? 0 : filterServerTable.hashCode());
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
        TopicRouteData other = (TopicRouteData) obj;
        if (brokerDatas == null) {
            if (other.brokerDatas != null) {
                return false;
            }
        } else if (!brokerDatas.equals(other.brokerDatas)) {
            return false;
        }
        if (orderTopicConf == null) {
            if (other.orderTopicConf != null) {
                return false;
            }
        } else if (!orderTopicConf.equals(other.orderTopicConf)) {
            return false;
        }
        if (queueDatas == null) {
            if (other.queueDatas != null) {
                return false;
            }
        } else if (!queueDatas.equals(other.queueDatas)) {
            return false;
        }
        if (filterServerTable == null) {
            return other.filterServerTable == null;
        } else {
            return filterServerTable.equals(other.filterServerTable);
        }
    }
}

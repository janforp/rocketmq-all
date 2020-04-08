package org.apache.rocketmq.common;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerConfigTest {

    @Test
    public void testConsumerFallBehindThresholdOverflow() {
        long expect = 1024L * 1024 * 1024 * 16;
        assertThat(new BrokerConfig().getConsumerFallbehindThreshold()).isEqualTo(expect);
    }

    @Test
    public void testBrokerConfigAttribute() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setNamesrvAddr("127.0.0.1:9876");
        brokerConfig.setAutoCreateTopicEnable(false);
        brokerConfig.setBrokerName("broker-a");
        brokerConfig.setBrokerId(0);
        brokerConfig.setBrokerClusterName("DefaultCluster");
        brokerConfig.setMsgTraceTopicName("RMQ_SYS_TRACE_TOPIC4");
        assertThat(brokerConfig.getBrokerClusterName()).isEqualTo("DefaultCluster");
        assertThat(brokerConfig.getNamesrvAddr()).isEqualTo("127.0.0.1:9876");
        assertThat(brokerConfig.getMsgTraceTopicName()).isEqualTo("RMQ_SYS_TRACE_TOPIC4");
        assertThat(brokerConfig.getBrokerId()).isEqualTo(0);
        assertThat(brokerConfig.getBrokerName()).isEqualTo("broker-a");
        assertThat(brokerConfig.isAutoCreateTopicEnable()).isEqualTo(false);
    }
}
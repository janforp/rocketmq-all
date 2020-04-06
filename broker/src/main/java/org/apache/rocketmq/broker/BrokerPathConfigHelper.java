package org.apache.rocketmq.broker;

import lombok.Getter;
import lombok.Setter;

import java.io.File;

public class BrokerPathConfigHelper {

    @Getter
    @Setter
    private static String brokerConfigPath = System.getProperty("user.home") + File.separator + "store" + File.separator + "config" + File.separator + "broker.properties";

    public static String getTopicConfigPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "topics.json";
    }

    public static String getConsumerOffsetPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "consumerOffset.json";
    }

    public static String getSubscriptionGroupPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "subscriptionGroup.json";
    }

    public static String getConsumerFilterPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "consumerFilter.json";
    }
}

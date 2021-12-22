package org.apache.rocketmq.common.filter;

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import java.net.URL;

public class FilterAPI {

    public static URL classFile(final String className) {
        final String javaSource = simpleClassName(className) + ".java";
        return FilterAPI.class.getClassLoader().getResource(javaSource);
    }

    public static String simpleClassName(final String className) {
        String simple = className;
        int index = className.lastIndexOf(".");
        if (index >= 0) {
            simple = className.substring(index + 1);
        }

        return simple;
    }

    /**
     * @param consumerGroup 组
     * @param topic 主题
     * @param subString tag等
     * @return SubscriptionData
     */
    public static SubscriptionData buildSubscriptionData(final String consumerGroup, String topic, String subString) throws Exception {
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);

        if (null == subString || subString.equals(SubscriptionData.SUB_ALL) || subString.length() == 0) {
            // 没有指定过滤表达式
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
        } else {
            // 指定了表达式

            // 按空格分开
            String[] tags = subString.split("\\|\\|");
            if (tags.length > 0) {
                for (String tag : tags) {
                    if (tag.length() > 0) {
                        String trimString = tag.trim();
                        if (trimString.length() > 0) {

                            // tag
                            subscriptionData.getTagsSet().add(trimString);

                            // hashcode
                            subscriptionData.getCodeSet().add(trimString.hashCode());
                        }
                    }
                }
            } else {
                throw new Exception("subString split error");
            }
        }

        return subscriptionData;
    }

    public static SubscriptionData build(final String topic, final String subString, final String type) throws Exception {
        if (ExpressionType.TAG.equals(type) || type == null) {
            return buildSubscriptionData(null, topic, subString);
        }

        if (subString == null || subString.length() < 1) {
            throw new IllegalArgumentException("Expression can't be null! " + type);
        }

        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        subscriptionData.setExpressionType(type);

        return subscriptionData;
    }
}

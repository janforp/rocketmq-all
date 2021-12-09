package org.apache.rocketmq.client.impl.producer;

import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;

import java.util.Set;

public interface MQProducerInner {

    /**
     * 获取生产者关心的topic
     * 也就是获取该生产者都有哪些topic的主题发布信息
     */
    Set<String> getPublishTopicList();

    /**
     * 判断指定topic是否需要更新主题发布信息
     */
    boolean isPublishTopicNeedUpdate(final String topic);

    TransactionCheckListener checkListener();

    TransactionListener getCheckListener();

    void checkTransactionState(final String addr, final MessageExt msg, final CheckTransactionStateRequestHeader checkRequestHeader);

    /**
     * 更新指定主题的发布信息
     */
    void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);

    boolean isUnitMode();
}
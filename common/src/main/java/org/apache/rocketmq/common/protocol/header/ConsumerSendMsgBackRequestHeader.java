package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@ToString
public class ConsumerSendMsgBackRequestHeader implements CommandCustomHeader {

    @CFNotNull
    @Getter
    @Setter // 回退消息在 commitLog 中的物理偏移量
    private Long offset;

    @CFNotNull
    @Getter
    @Setter // 回退组
    private String group;

    /**
     * -1:直接扔进死信队列
     * 0:broker 控制延迟级别
     * >0:客户端控制延迟级别
     */
    @CFNotNull
    @Getter
    @Setter // 延迟级别
    private Integer delayLevel;

    @Getter
    @Setter // 原始消息的id
    private String originMsgId;

    @Getter
    @Setter // 原始主题
    private String originTopic;

    @CFNullable
    @Getter
    @Setter
    private boolean unitMode = false;

    @Getter
    @Setter // 最大重复消费次数，默认16
    private Integer maxReconsumeTimes;

    @Override
    public void checkFields() throws RemotingCommandException {

    }
}
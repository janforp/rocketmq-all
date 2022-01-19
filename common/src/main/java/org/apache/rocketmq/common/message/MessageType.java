package org.apache.rocketmq.common.message;

public enum MessageType {

    // 正常消息
    Normal_Msg,

    // 事务消息的半消息
    Trans_Msg_Half,

    // 事务消息的提交消息
    Trans_msg_Commit,

    // 延迟消息
    Delay_Msg,
}

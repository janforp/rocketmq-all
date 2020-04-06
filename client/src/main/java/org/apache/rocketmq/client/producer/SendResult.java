package org.apache.rocketmq.client.producer;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageQueue;

@NoArgsConstructor
public class SendResult {

    @Getter
    @Setter
    private SendStatus sendStatus;

    @Getter
    @Setter
    private String msgId;

    @Getter
    @Setter
    private MessageQueue messageQueue;

    @Getter
    @Setter
    private long queueOffset;

    @Getter
    @Setter
    private String transactionId;

    @Getter
    @Setter
    private String offsetMsgId;

    @Getter
    @Setter
    private String regionId;

    @Getter
    @Setter
    private boolean traceOn = true;

    public SendResult(SendStatus sendStatus, String msgId, String offsetMsgId, MessageQueue messageQueue, long queueOffset) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
        this.offsetMsgId = offsetMsgId;
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
    }

    public SendResult(final SendStatus sendStatus, final String msgId, final MessageQueue messageQueue, final long queueOffset,
            final String transactionId, final String offsetMsgId, final String regionId) {

        this.sendStatus = sendStatus;
        this.msgId = msgId;
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
        this.transactionId = transactionId;
        this.offsetMsgId = offsetMsgId;
        this.regionId = regionId;
    }

    public static String encoderSendResultToJson(final Object obj) {
        return JSON.toJSONString(obj);
    }

    public static SendResult decoderSendResultFromJson(String json) {
        return JSON.parseObject(json, SendResult.class);
    }

    @Override
    public String toString() {
        return "SendResult [sendStatus=" + sendStatus + ", msgId=" + msgId + ", offsetMsgId=" + offsetMsgId + ", messageQueue=" + messageQueue
                + ", queueOffset=" + queueOffset + "]";
    }
}

package org.apache.rocketmq.store;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@AllArgsConstructor
public class PutMessageResult {

    @Getter
    @Setter
    private PutMessageStatus putMessageStatus;

    /**
     * 追加消息内容到 commitLog 文件的结果
     */
    @Getter
    @Setter
    private AppendMessageResult appendMessageResult;

    public boolean isOk() {
        return this.appendMessageResult != null && this.appendMessageResult.isOk();
    }
}
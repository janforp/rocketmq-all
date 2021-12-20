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

    @Getter
    @Setter
    private AppendMessageResult appendMessageResult;

    public boolean isOk() {
        return this.appendMessageResult != null && this.appendMessageResult.isOk();
    }
}
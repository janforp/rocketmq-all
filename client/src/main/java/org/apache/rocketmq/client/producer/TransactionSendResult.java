package org.apache.rocketmq.client.producer;

import lombok.Getter;
import lombok.Setter;

public class TransactionSendResult extends SendResult {

    @Setter
    @Getter
    private LocalTransactionState localTransactionState;

    public TransactionSendResult() {
    }
}

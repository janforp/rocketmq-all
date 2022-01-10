package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

/**
 * 二阶段提交 header
 */
public class EndTransactionRequestHeader implements CommandCustomHeader {

    @CFNotNull
    @Setter
    @Getter
    private String producerGroup;

    @CFNotNull
    @Setter
    @Getter
    private Long tranStateTableOffset;

    @CFNotNull
    @Setter
    @Getter
    private Long commitLogOffset;

    @CFNotNull
    private Integer commitOrRollback; // TRANSACTION_COMMIT_TYPE
    // TRANSACTION_ROLLBACK_TYPE
    // TRANSACTION_NOT_TYPE

    @CFNullable
    @Setter
    @Getter
    private Boolean fromTransactionCheck = false;

    @CFNotNull
    @Setter
    @Getter
    private String msgId;

    @Setter
    @Getter
    private String transactionId;

    @Override
    public void checkFields() throws RemotingCommandException {
        if (MessageSysFlag.TRANSACTION_NOT_TYPE == this.commitOrRollback) {
            return;
        }

        if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == this.commitOrRollback) {
            return;
        }

        if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == this.commitOrRollback) {
            return;
        }

        throw new RemotingCommandException("commitOrRollback field wrong");
    }

    public void setCommitOrRollback(Integer commitOrRollback) {
        this.commitOrRollback = commitOrRollback;
    }
}
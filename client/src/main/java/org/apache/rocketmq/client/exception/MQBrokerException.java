package org.apache.rocketmq.client.exception;

import lombok.Getter;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;

public class MQBrokerException extends Exception {

    private static final long serialVersionUID = 5975020272601250368L;

    @Getter
    private final int responseCode;

    @Getter
    private final String errorMessage;

    public MQBrokerException(int responseCode, String errorMessage) {
        super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: "
                + errorMessage));
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }
}

package org.apache.rocketmq.client.exception;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;

public class MQClientException extends Exception {

    private static final long serialVersionUID = -5758410930844185841L;

    @Getter
    private int responseCode;

    @Getter
    @Setter
    private String errorMessage;

    public MQClientException(String errorMessage, Throwable cause) {
        super(FAQUrl.attachDefaultURL(errorMessage), cause);
        this.responseCode = -1;
        this.errorMessage = errorMessage;
    }

    public MQClientException(int responseCode, String errorMessage) {
        super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: "
                + errorMessage));
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

    public MQClientException setResponseCode(final int responseCode) {
        this.responseCode = responseCode;
        return this;
    }
}

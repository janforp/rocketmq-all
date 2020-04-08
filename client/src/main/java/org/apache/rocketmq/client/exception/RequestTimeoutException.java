package org.apache.rocketmq.client.exception;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.UtilAll;

public class RequestTimeoutException extends Exception {

    private static final long serialVersionUID = -5758410930844185841L;

    @Getter
    private int responseCode;

    @Getter
    @Setter
    private String errorMessage;

    public RequestTimeoutException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
        this.responseCode = -1;
        this.errorMessage = errorMessage;
    }

    public RequestTimeoutException(int responseCode, String errorMessage) {
        super("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: "
                + errorMessage);
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

    public RequestTimeoutException setResponseCode(final int responseCode) {
        this.responseCode = responseCode;
        return this;
    }
}

package org.apache.rocketmq.remoting.exception;

public class RemotingSendRequestException extends RemotingException {

    private static final long serialVersionUID = 5391285827332471674L;

    public RemotingSendRequestException(String addr) {
        this(addr, null);
    }

    public RemotingSendRequestException(String addr, Throwable cause) {
        // String message = "send request to <" + addr + "> failed";
        //发送请求失败
        super("send request to <" + addr + "> failed", cause);
    }
}

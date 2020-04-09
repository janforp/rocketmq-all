package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public interface CommandCustomHeader {

    void checkFields() throws RemotingCommandException;
}

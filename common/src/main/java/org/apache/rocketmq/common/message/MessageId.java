package org.apache.rocketmq.common.message;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.net.SocketAddress;

@Data
@AllArgsConstructor
public class MessageId {

    private SocketAddress address;

    private long offset;
}
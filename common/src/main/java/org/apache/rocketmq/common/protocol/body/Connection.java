package org.apache.rocketmq.common.protocol.body;

import lombok.Data;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

@Data
public class Connection {

    private String clientId;

    private String clientAddr;

    private LanguageCode language;

    private int version;
}

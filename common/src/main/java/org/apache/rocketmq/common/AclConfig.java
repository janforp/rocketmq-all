package org.apache.rocketmq.common;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

public class AclConfig {

    @Getter
    @Setter
    private List<String> globalWhiteAddrs;

    @Getter
    @Setter
    private List<PlainAccessConfig> plainAccessConfigs;
}

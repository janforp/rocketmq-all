package org.apache.rocketmq.common.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.MixAll;

import java.io.File;

public class NamesrvConfig {

    @Getter
    @Setter
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    @Getter
    @Setter
    private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "kvConfig.json";

    @Getter
    @Setter
    private String configStorePath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "namesrv.properties";

    @Getter
    @Setter
    private String productEnvName = "center";

    @Getter
    @Setter
    private boolean clusterTest = false;

    @Getter
    @Setter
    private boolean orderMessageEnable = false;
}

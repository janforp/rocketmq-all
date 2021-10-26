package org.apache.rocketmq.common.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.MixAll;

import java.io.File;

public class NamesrvConfig {

    /**
     * rocketmq.home.dir
     */
    @Getter
    @Setter
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    /**
     * kvConfig.json 文件
     */
    @Getter
    @Setter
    private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "kvConfig.json";

    /**
     * namesrv.properties：一般不用
     */
    @Getter
    @Setter
    private String configStorePath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "namesrv.properties";

    @Getter
    @Setter
    private String productEnvName = "center";

    @Getter
    @Setter
    private boolean clusterTest = false;

    /**
     * 顺序消息是否开启
     */
    @Getter
    @Setter
    private boolean orderMessageEnable = false;
}

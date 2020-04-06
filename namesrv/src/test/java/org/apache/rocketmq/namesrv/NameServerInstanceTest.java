package org.apache.rocketmq.namesrv;

import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.junit.After;
import org.junit.Before;

import static org.assertj.core.api.Assertions.assertThat;

public class NameServerInstanceTest {

    protected NamesrvController nameSrvController = null;

    protected NettyServerConfig nettyServerConfig = new NettyServerConfig();

    protected NamesrvConfig namesrvConfig = new NamesrvConfig();

    @Before
    public void startup() throws Exception {
        nettyServerConfig.setListenPort(9876);
        nameSrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
        boolean initResult = nameSrvController.initialize();
        assertThat(initResult).isTrue();
        nameSrvController.start();
    }

    @After
    public void shutdown() throws Exception {
        if (nameSrvController != null) {
            nameSrvController.shutdown();
        }
        //maybe need to clean the file store. But we do not suggest deleting anything.
    }
}

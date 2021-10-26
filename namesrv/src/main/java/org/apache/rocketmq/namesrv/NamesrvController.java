package org.apache.rocketmq.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * nameserv的主要控制类，负责初始化和后台任务启动，Controller包含的主要组件都在构造函数中做了初始化
 */
public class NamesrvController {

    /**
     * RocketmqNamesrv
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    @Getter
    private final NamesrvConfig namesrvConfig;

    @Getter
    private final NettyServerConfig nettyServerConfig;

    /**
     * 执行定时任务
     * 1.检查broker存活状态
     * 2.打印配置
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("NSScheduledThread"));

    /**
     * 管理kv配置
     */
    @Getter
    private final KVConfigManager kvConfigManager;

    /**
     * 管理路由信息的对象，比较重要
     */
    @Getter
    private final RouteInfoManager routeInfoManager;

    /**
     * 网络层封装对象，重要
     */
    @Getter
    @Setter
    private RemotingServer remotingServer;

    /**
     * @see ChannelEventListener
     * 一个org.apache.rocketmq.remoting.ChannelEventListener实例
     * 用于监听 channel 状态，当 channel 状态发生变化时，如 close, idle ... 会向事件队列
     * 发送事件，事件最终由该 service 处理
     */
    private final BrokerHousekeepingService brokerHousekeepingService;

    // 业务线程池
    private ExecutorService remotingExecutor;

    @Getter
    private final Configuration configuration;

    // 安全
    private FileWatchService fileWatchService;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.kvConfigManager = new KVConfigManager(this);
        this.routeInfoManager = new RouteInfoManager();
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.configuration = new Configuration(log, this.namesrvConfig, this.nettyServerConfig);
        this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
    }

    public boolean initialize() {

        this.kvConfigManager.load();

        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);

        this.remotingExecutor = Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));

        this.registerProcessor();

        // 定时任务1，每10s检查 broker 存活状态，将 idle 状态的 Broker 移除
        this.scheduledExecutorService.scheduleAtFixedRate(NamesrvController.this.routeInfoManager::scanNotActiveBroker, 5, 10, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(NamesrvController.this.kvConfigManager::printAllPeriodically, 1, 10, TimeUnit.MINUTES);

        if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
            // Register a listener to reload SslContext
            try {
                fileWatchService = new FileWatchService(
                        new String[] {
                                TlsSystemConfig.tlsServerCertPath,
                                TlsSystemConfig.tlsServerKeyPath,
                                TlsSystemConfig.tlsServerTrustCertPath
                        },
                        new FileWatchService.Listener() {

                            boolean certChanged = false;

                            boolean keyChanged = false;

                            @Override
                            public void onChanged(String path) {
                                if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                                    log.info("The trust certificate changed, reload the ssl context");
                                    reloadServerSslContext();
                                }
                                if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                                    certChanged = true;
                                }
                                if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                                    keyChanged = true;
                                }
                                if (certChanged && keyChanged) {
                                    log.info("The certificate and private key changed, reload the ssl context");
                                    certChanged = keyChanged = false;
                                    reloadServerSslContext();
                                }
                            }

                            private void reloadServerSslContext() {
                                ((NettyRemotingServer) remotingServer).loadSslContext();
                            }
                        });
            } catch (Exception e) {
                log.warn("FileWatchService created error, can't load the certificate dynamically");
            }
        }

        return true;
    }

    private void registerProcessor() {
        if (namesrvConfig.isClusterTest()) {
            String productEnvName = namesrvConfig.getProductEnvName();
            ClusterTestRequestProcessor clusterTestRequestProcessor = new ClusterTestRequestProcessor(this, productEnvName);
            this.remotingServer.registerDefaultProcessor(clusterTestRequestProcessor, this.remotingExecutor);
        } else {
            DefaultRequestProcessor defaultRequestProcessor = new DefaultRequestProcessor(this);
            this.remotingServer.registerDefaultProcessor(defaultRequestProcessor, this.remotingExecutor);
        }
    }

    public void start() throws Exception {
        //服务器 网络层 启动
        this.remotingServer.start();
        if (this.fileWatchService != null) {
            this.fileWatchService.start();
        }
    }

    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }
    }
}

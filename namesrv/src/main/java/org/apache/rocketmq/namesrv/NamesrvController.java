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

    // 配置文件加载进来
    @Getter
    private final NamesrvConfig namesrvConfig;

    // 配置文件加载进来
    @Getter
    private final NettyServerConfig nettyServerConfig;

    /**
     * 执行定时任务
     * 1.检查broker存活状态，移除已经掉线的 broker
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
     * @see RouteInfoManager#onChannelDestroy(java.lang.String, io.netty.channel.Channel) 最终还是调用这个方法
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

    /**
     * 配置完后,接着就是启动NamesrvController.首先是初始化.初始化initialize的动作稍有复杂.主要做了
     *
     * KvConfigManager的加载
     * RemotingServer加载和启动
     * 注册处理器.(处理器指的是处理访问当前计算机处理请求的逻辑)
     * 开启两个定时器:扫描不活跃的Broker以及打印kvConfigManager的参数情况
     * 启动FileWatchService,用于SSL/TLS
     */
    public boolean initialize() {
        // 开始加载对应的参数
        this.kvConfigManager.load();
        // RemotingServer是负责接受来自Broker的注册网络通信
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService/*连接销毁，断开的时候的回调方法*/);
        this.remotingExecutor = Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads()/*默认8，可以配置*/, new ThreadFactoryImpl("RemotingExecutorThread_"));
        // 注册协议处理器
        this.registerProcessor();
        // 定时任务1，每10s检查 broker 存活状态，将 idle 状态的 Broker 移除
        // //定时扫面不活跃Broker
        this.scheduledExecutorService.scheduleAtFixedRate(NamesrvController.this.routeInfoManager::scanNotActiveBroker/*检查存活状态*/, 5, 10, TimeUnit.SECONDS);
        // //定时打印KV值
        this.scheduledExecutorService.scheduleAtFixedRate(NamesrvController.this.kvConfigManager::printAllPeriodically/*打印配置*/, 1, 10, TimeUnit.MINUTES);

        // 最后就是处理关于SSL/TLS的事情.说明一下,RocketMQ需要定义路径:CertPath、KeyPath和TrustCertPath的地址,是为了写入文件.如果没有权限会报错的.
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

                            // //证书更改、键值更改
                            boolean certChanged = false;

                            // //证书更改、键值更改
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

            // 注册一个默认的请求处理器
            DefaultRequestProcessor defaultRequestProcessor = new DefaultRequestProcessor(this);
            this.remotingServer.registerDefaultProcessor(defaultRequestProcessor, this.remotingExecutor);
        }
    }

    public void start() throws Exception {
        //服务器 网络层 启动
        //获取netty的remoteServer
        this.remotingServer.start();
        if (this.fileWatchService != null) {
            //文件监视服务开启
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

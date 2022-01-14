package org.apache.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import lombok.Getter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * NamesrvStartup是启动类.主要的作用是解析来自命令行的参数、配置namesrv和netty的配置文件解析、配置日志上下文以及配置NamesrvController最后启动.
 */
public class NamesrvStartup {

    private static InternalLogger log;

    /**
     * *      if (commandLine.hasOption('c')) {
     * *             // 如果有 -c 选项
     * *             // 读取
     * *             String file = commandLine.getOptionValue('c');
     * *             if (file != null) {
     * *                 // 读取
     * *                 InputStream in = new BufferedInputStream(new FileInputStream(file));
     * *                 properties = new Properties();
     * *                 properties.load(in);
     * *                 MixAll.properties2Object(properties, namesrvConfig);
     * *                 MixAll.properties2Object(properties, nettyServerConfig);
     * *
     * *                 namesrvConfig.setConfigStorePath(file);
     * *
     * *                 System.out.printf("load config properties file OK, %s%n", file);
     * *                 in.close();
     * *             }
     * *         }
     */
    @Getter
    private static Properties properties = null;

    /**
     * -c /Users/zhuchenjian/Documents/code/learn/rocketmq/rocketmq-all/conf/home/nameerver/nameserver.properties
     *
     * @param args 参数
     */
    public static void main(String[] args) {
        // 如果启动时使用了 -c -p ... 设置了参数，这些参数由 args 承接
        NamesrvController controller = main0(args);
        System.out.println(controller);
    }

    public static NamesrvController main0(String[] args) {

        try {
            /*
             * 创建 namesrv 控制器：
             * namesrv 控制器：初始化 namesrv,启动namesrv，关闭namesrv
             *
             * 这个方式是用于参数配置,例如将启动命令行的参数获取、从配置文件读取默认配置等.
             */
            NamesrvController controller = createNamesrvController(args);
            start(controller);
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            System.out.println("nameserver 启动成功");
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /***
     * 读取配置信息，初始化控制器
     */
    private static NamesrvController createNamesrvController(String[] args) throws IOException, JoranException {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY/*rocketmq.remoting.version*/, Integer.toString(MQVersion.CURRENT_VERSION));

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        /*
         * 非核心：启动参数信息，由commandLine管理
         * 命令行
         */
        CommandLine commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }

        // 先实例话，后面通过配置文件向里面设置属性
        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        // 服务器配置
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        //namesrv服务端启动监听端口
        nettyServerConfig.setListenPort(9876);

        // 根据参数去填补对应的NamesrvConfig对象以及NettyServerConfig对象.
        if (commandLine.hasOption('c')) {
            // 如果有 -c 选项
            // 读取
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                // 读取 config 文件数据到 properties 内
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                properties.load(in);

                // 如果 config 配置文件内的配置涉及到 namesrvConfig 或者 nettyServerConfig 的字段，则进行覆盖
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);

                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        if (commandLine.hasOption('p')) {
            // 不会进来
            InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_NAME);
            MixAll.printObjectProperties(console, namesrvConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            System.exit(0);
        }

        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

        if (null == namesrvConfig.getRocketmqHome()) {
            // 不会进来
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        // 接着获取RocketMq的目录,为产生日志的位置做好准备

        // //实例化日志上下文
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");
        log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);

        // 创建控制器
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

        // remember all configs to prevent discard
        // 记住所有的配置都不应该忽视
        Configuration configuration = controller.getConfiguration();
        configuration.registerConfig(properties);

        return controller;
    }

    public static NamesrvController start(final NamesrvController controller) throws Exception {
        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }
        boolean initResult = controller.initialize();
        if (!initResult) {
            // namesrv 初始化失败！！
            controller.shutdown();
            System.exit(-3);
        }

        Runtime runtime = Runtime.getRuntime();
        runtime.addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {

            // 钩子
            controller.shutdown();
            return null;
        }));
        controller.start();
        return controller;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
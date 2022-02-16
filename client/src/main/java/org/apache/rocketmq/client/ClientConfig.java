package org.apache.rocketmq.client;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.utils.NameServerAddressUtils;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Client Common configuration
 */
@ToString
public class ClientConfig {

    public static final String SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY = "com.rocketmq.sendMessageWithVIPChannel";

    /**
     * Domain name mode access way does not support the delimiter(;), and only one domain name can be set.
     * 当前的 nameserver 地址配置
     */
    @Setter
    private String namesrvAddr = NameServerAddressUtils.getNameServerAddresses();

    // 当前客户端ip
    @Getter
    @Setter
    private String clientIP = RemotingUtil.getLocalAddress();

    // 当前客户端实例名称
    @Getter
    @Setter
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");

    /**
     * 返回到Java虚拟机的可用的处理器数量
     *
     * 当前客户端回调线程池线程数量
     */
    @Getter
    @Setter
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();

    // 命名空间,一般用于区分 环境，如：dev,pre,prod 等
    @Setter
    protected String namespace;

    @Getter
    @Setter
    protected AccessChannel accessChannel = AccessChannel.LOCAL;

    /**
     * Pulling topic information interval from the named server
     * 获取路由信息的间隔时间
     *
     * @see MQClientInstance#updateTopicRouteInfoFromNameServer()
     */
    @Getter
    @Setter
    private int pollNameServerInterval = 1000 * 30;

    /**
     * Heartbeat interval in microseconds with message broker
     * 客户端与broker心跳的间隔时间
     *
     * @see MQClientInstance#sendHeartbeatToAllBrokerWithLock()
     */
    @Getter
    @Setter
    private int heartbeatBrokerInterval = 1000 * 30;

    /**
     * Offset persistent interval for consumer
     * 消费者持久化消费进度的周期，5s一次
     *
     * @see MQClientInstance#persistAllConsumerOffset()
     */
    @Getter
    @Setter
    private int persistConsumerOffsetInterval = 1000 * 5;

    @Getter
    @Setter
    private long pullTimeDelayMillsWhenException = 1000;

    // 唯一模式？？
    @Getter
    @Setter
    private boolean unitMode = false;

    /**
     * @see ClientConfig#buildMQClientId() 控制客户端实例是否唯一
     */
    @Getter
    @Setter
    private String unitName;

    /**
     * 是否启用vip netty通道以发送消息
     * broker启动的时候会绑定2个端口，其中一个是vip通道端口
     *
     * 如果某个生产者开启了vip通过，则与broker连接的时候使用的是 broker 的另外一个端口进行网络连接！！！！
     */
    @Getter
    @Setter
    private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false"));

    @Getter
    @Setter
    private boolean useTLS = TlsSystemConfig.tlsEnable;

    @Getter
    @Setter
    private LanguageCode language = LanguageCode.JAVA;

    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();

        String clientIP = this.getClientIP(); // 10.201.13.28
        sb.append(clientIP);

        sb.append("@"); // 10.201.13.28@

        String instanceName = this.getInstanceName(); // 9738
        sb.append(instanceName); // 10.201.13.28@9738

        if (!UtilAll.isBlank(this.unitName)) {
            sb.append("@");
            sb.append(this.unitName);
        }

        return sb.toString();
    }

    public void changeInstanceNameToPID() {

        // String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
        if ("DEFAULT".equals(this.instanceName)) {
            int pid = UtilAll.getPid();
            this.instanceName = String.valueOf(pid);
        }
    }

    // 如果又 namespace，则加上 namespace% 返回
    public String withNamespace(String resource) {
        String namespace = this.getNamespace();
        return NamespaceUtil.wrapNamespace(namespace, resource);
    }

    public Set<String> withNamespace(Set<String> resourceSet) {
        Set<String> resourceWithNamespace = new HashSet<String>();
        for (String resource : resourceSet) {

            String withNamespace = withNamespace(resource);
            resourceWithNamespace.add(withNamespace);
        }
        return resourceWithNamespace;
    }

    public String withoutNamespace(String resource) {
        String namespace = this.getNamespace();
        return NamespaceUtil.withoutNamespace(resource, namespace);
    }

    public Set<String> withoutNamespace(Set<String> resourceSet) {
        Set<String> resourceWithoutNamespace = new HashSet<String>();
        for (String resource : resourceSet) {

            String withoutNamespace = withoutNamespace(resource);
            resourceWithoutNamespace.add(withoutNamespace);
        }
        return resourceWithoutNamespace;
    }

    public MessageQueue queueWithNamespace(MessageQueue queue) {
        String namespace = this.getNamespace();
        if (StringUtils.isEmpty(namespace)) {
            return queue;
        }

        String topic = queue.getTopic();
        String brokerName = queue.getBrokerName();
        int queueId = queue.getQueueId();

        String topicWithNamespace = withNamespace(topic);
        return new MessageQueue(topicWithNamespace, brokerName, queueId);
    }

    public Collection<MessageQueue> queuesWithNamespace(Collection<MessageQueue> queues) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queues;
        }
        for (MessageQueue queue : queues) {
            queue.setTopic(withNamespace(queue.getTopic()));
        }
        return queues;
    }

    public void resetClientConfig(final ClientConfig cc) {
        this.namesrvAddr = cc.namesrvAddr;
        this.clientIP = cc.clientIP;
        this.instanceName = cc.instanceName;
        this.clientCallbackExecutorThreads = cc.clientCallbackExecutorThreads;
        this.pollNameServerInterval = cc.pollNameServerInterval;
        this.heartbeatBrokerInterval = cc.heartbeatBrokerInterval;
        this.persistConsumerOffsetInterval = cc.persistConsumerOffsetInterval;
        this.pullTimeDelayMillsWhenException = cc.pullTimeDelayMillsWhenException;
        this.unitMode = cc.unitMode;
        this.unitName = cc.unitName;
        this.vipChannelEnabled = cc.vipChannelEnabled;
        this.useTLS = cc.useTLS;
        this.namespace = cc.namespace;
        this.language = cc.language;
    }

    public ClientConfig cloneClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.namesrvAddr = namesrvAddr;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
        cc.pollNameServerInterval = pollNameServerInterval;
        cc.heartbeatBrokerInterval = heartbeatBrokerInterval;
        cc.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
        cc.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
        cc.unitMode = unitMode;
        cc.unitName = unitName;
        cc.vipChannelEnabled = vipChannelEnabled;
        cc.useTLS = useTLS;
        cc.namespace = namespace;
        cc.language = language;
        return cc;
    }

    public String getNamesrvAddr() {
        if (StringUtils.isNotEmpty(namesrvAddr) && NameServerAddressUtils.NAMESRV_ENDPOINT_PATTERN.matcher(namesrvAddr.trim()).matches()) {
            return namesrvAddr.substring(NameServerAddressUtils.ENDPOINT_PREFIX.length());
        }
        return namesrvAddr;
    }

    public String getNamespace() {
        if (StringUtils.isNotEmpty(namespace)) {
            return namespace;
        }

        if (StringUtils.isNotEmpty(this.namesrvAddr)) {
            if (NameServerAddressUtils.validateInstanceEndpoint(namesrvAddr)) {
                return NameServerAddressUtils.parseInstanceIdFromEndpoint(namesrvAddr);
            }
        }
        return namespace;
    }
}

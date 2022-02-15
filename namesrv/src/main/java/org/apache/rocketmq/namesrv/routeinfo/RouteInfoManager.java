package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 这个最重要的类，负责缓存整个集群的broker信息，以及topic和queue的配置信息。
 * RouteInfoManager的所有数据通过HashMap缓存在内存中，通过读写锁来控制并发更新。
 * 这样可最大程度的提高客户端查询数据的速度。
 * 数据更新时会将数据保存到文件中，重启后可恢复数据。
 * <p>
 * <p>
 * 成员属性里面涉及几个数据结构QueueData、BrokerData、BrokerLiveInfo.它们有以下的描述:
 * <p>
 * 一个Topic拥有多个消息队列,一个Broker为每个Topic默认创建4个读队列4个写对列;
 * 多个Broker组成一个集群,BrokerName相同的即可组成Master-slave架构;
 * BrokerLiveInfo中的lastUpdateTimestamp存的是最近一次心跳的时间;
 */
@SuppressWarnings("all")
public class RouteInfoManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    // broker 超时事件：2 分钟
    private final static long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * 主题分布
     * <p>
     * Topic和broker的Map，保存了topic在每个broker上的读写Queue的个数以及读写权限
     * <p>
     * 消息队列路由消息,消息发送会根据路由表负责均衡
     *
     * 集群部署的时候，topic 是可以分布在不同的 brokerName 上的！！！！
     */
    private final HashMap<String/* topic */, List<QueueData/*包含 broker 的队列信息*/> /*该主题下面的各个队列的属性*/> topicQueueTable;

    /**
     * broker 地址 映射表
     *
     * 通过 brokerName ,通过这个映射表可以查询到该 brokerName 对应的主节点的地址，进行网络通信
     *
     * 注册到nameserv上的所有Broker，按照brokername分组
     * <p>
     * Broker基础信息.所在集群名称、brokerName以及主备Broker地址
     */
    private final HashMap<String/* brokerName，如：broker-a */, BrokerData /* broker 信息,包括主从 broker，只要 brokerName 相关的 broker 都会封装在一个对象中，其实都是由配置决定 */> brokerAddrTable;

    /**
     * broker的集群对应关系
     * <p>
     * Broker集群信息,存储各个集群下所有Broker的名称
     *
     * #broker名字，注意此处不同的配置文件填写的不一样
     * brokerName=broker-a
     */
    private final HashMap<String/* clusterName 集群名称 */, Set<String/* brokerName */> /*某个集群下面的所有 broker 名称集合*/> clusterAddrTable;

    /**
     * broker 存活状态！！！！
     * broker最新的心跳时间和配置版本号
     * <p>
     * Broker状态信息,心跳包会更新，
     * 每次 broker 向 namesrv 发送心跳之后，就会更新这个映射表
     *
     * @see RouteInfoManager#scanNotActiveBroker()
     */
    private final HashMap<String/* brokerAddr，如 127.0.0.1:10911 在整个集群中是唯一的 */, BrokerLiveInfo /*封装了该broker最近心跳的时间,broker是否存活也依赖该时间*/> brokerLiveTable;

    /**
     * broker和FilterServer的对应关系
     * <p>
     * filterServer 列表,用于类模式消息过滤
     */
    private final HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    public RouteInfoManager() {
        this.topicQueueTable = new HashMap<>(1024);
        this.brokerAddrTable = new HashMap<>(128);
        this.clusterAddrTable = new HashMap<>(32);
        this.brokerLiveTable = new HashMap<>(256);
        this.filterServerTable = new HashMap<>(256);
    }

    public byte[] getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper.encode();
    }

    public void deleteTopic(final String topic) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                this.topicQueueTable.remove(topic);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        }
    }

    public byte[] getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();

                // Set<String> topicList = new HashSet<String>();
                Set<String> topicSet = topicList.getTopicList();
                //HashMap<String/* topic */, List<QueueData> /*该主题下面的各个队列的属性*/> topicQueueTable;
                Set<String> keySet = this.topicQueueTable.keySet();
                topicSet.addAll(keySet);
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    /**
     * broker 启动的时候会把自己注册到每个 namesrv 上
     *
     * @see org.apache.rocketmq.broker.out.BrokerOuterAPI#registerBroker(java.lang.String, boolean, int, org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader, byte[])
     */
    // crud
    public RegisterBrokerResult registerBroker(
            final String clusterName/*集群名称*/, final String brokerAddr/*节点ip地址*/, final String brokerName/*节点名称*/, final long brokerId/*节点id:0为主节点*/, final String haServerAddr/*ha节点地址*/,
            final TopicConfigSerializeWrapper topicConfigWrapper/*当前broker节点的主题信息*/, final List<String> filterServerList/*过滤服务器列表*/, final Channel channel/*当前服务端与客户端的通信ch*/) {
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                // 获取当前集群上的 broker 列表
                // HashMap<String/* clusterName 集群名称 */, Set<String/* brokerName */> /*某个集群下面的所有 broker 名称集合*/> clusterAddrTable;
                Set<String> brokerNames = this.clusterAddrTable.computeIfAbsent(clusterName, k -> new HashSet<>() /* 如果当前 集群名称 还没有对于的 Set，则新创建一个空集合赛进去 */);
                brokerNames.add(brokerName);

                // broker是否首次注册？因为一个 brokerName 可能存在多台机器，他们的 name 相同，只有第一次注册的时候 registerFirst 为 true
                boolean registerFirst = false;
                // HashMap<String/* brokerName，如：broker-a */, BrokerData /* broker 信息,包括主从 broker，只要 brokerName 相关的 broker 都会封装在一个对象中，其实都是有配置决定 */> brokerAddrTable;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null == brokerData) {
                    // 说明当前 broker 是首次注册
                    registerFirst = true;
                    // 则创建 brokerData
                    brokerData = new BrokerData(clusterName, brokerName, new HashMap<>());
                    this.brokerAddrTable.put(brokerName, brokerData);
                }

                // 获取物理节点数据 map 表
                Map<Long/* brokerId */, String/* broker address */> brokerAddrsMap = brokerData.getBrokerAddrs();
                //Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
                //The same IP:PORT must only have one record in brokerAddrTable

                // 遍历
                // 当前物理节点角色发生变化了，此时需要将它从 broker 物理节点映射表中移除，后面会重新写进去
                brokerAddrsMap.entrySet().removeIf(item -> null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey()/* brokeId 发生了变化 */);

                // 重写
                HashMap<Long, String> brokerDataBrokerAddrs = brokerData.getBrokerAddrs();
                // 把地址添加到映射表
                String oldAddr = brokerDataBrokerAddrs.put(brokerId, brokerAddr);
                registerFirst = registerFirst || (null == oldAddr);

                if (null != topicConfigWrapper/*当前broker节点的主题信息*/ && MixAll.MASTER_ID == brokerId/*当前物理节点是主节点*/) {
                    DataVersion dataVersion = topicConfigWrapper.getDataVersion();
                    boolean brokerTopicConfigChanged = this.isBrokerTopicConfigChanged(brokerAddr, dataVersion/*其实就是比较版本是否一致*/);
                    if (brokerTopicConfigChanged || registerFirst) {
                        // 获取主题映射表，从 broker 注册数据中获取
                        ConcurrentMap<String/*topic*/ , TopicConfig/*主题信息*/> tcTable = topicConfigWrapper.getTopicConfigTable();
                        if (tcTable != null) {
                            // 加入 或者 更新到 namesrv中
                            for (Map.Entry<String/*topic*/, TopicConfig> entry : tcTable.entrySet()) {
                                TopicConfig topicConfig = entry.getValue();
                                // private final HashMap<String/* topic */, List<QueueData/*包含 broker 的队列信息*/> /*该主题下面的各个队列的属性*/> topicQueueTable;
                                this.createAndUpdateQueueData(brokerName, topicConfig/*每个主题的信息*/);
                            }
                        }
                    }
                }

                // 定时任务会扫描该表

                // 返回上次心态时 当前 broker 节点的存活数据对象
                DataVersion dataVersion = topicConfigWrapper.getDataVersion();
                BrokerLiveInfo liveInfo = new BrokerLiveInfo(System.currentTimeMillis()/*时间设置为现在*/, dataVersion, channel, haServerAddr);
                BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr, liveInfo);
                if (null == prevBrokerLiveInfo) {
                    // 说明时新注册
                    log.info("new broker registered, {} HAServer: {}", brokerAddr, haServerAddr);
                }

                if (filterServerList != null) {
                    if (filterServerList.isEmpty()) {
                        this.filterServerTable.remove(brokerAddr);
                    } else {
                        this.filterServerTable.put(brokerAddr, filterServerList);
                    }
                }

                // 不是主节点
                if (MixAll.MASTER_ID != brokerId) {
                    // 不是主节点
                    // 则需要拿到master的物理地址
                    HashMap<Long, String> brokerAddrs = brokerData.getBrokerAddrs();
                    String masterAddr = brokerAddrs.get(MixAll.MASTER_ID);
                    if (masterAddr != null) {
                        // HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
                        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(masterAddr);
                        if (brokerLiveInfo != null) {
                            String liveInfoHaServerAddr = brokerLiveInfo.getHaServerAddr();
                            result.setHaServerAddr(liveInfoHaServerAddr);
                            result.setMasterAddr(masterAddr);
                        }
                    }
                }
            } finally {
                // 释放锁
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        }
        return result;
    }

    public boolean isBrokerTopicConfigChanged(final String brokerAddr, final DataVersion dataVersion) {
        DataVersion prev = queryBrokerTopicConfig(brokerAddr);
        return null == prev || !prev.equals(dataVersion);
    }

    public DataVersion queryBrokerTopicConfig(final String brokerAddr) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            return prev.getDataVersion();
        }
        return null;
    }

    public void updateBrokerInfoUpdateTimestamp(final String brokerAddr) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            prev.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName /* 配置 broker-a*/);
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSynFlag(topicConfig.getTopicSysFlag());
        String topicName = topicConfig.getTopicName();
        // HashMap<String/* topic */, List<QueueData> /*该主题下面的各个队列的属性*/> topicQueueTable
        List<QueueData> queueDataList = this.topicQueueTable.get(topicName);
        if (null == queueDataList) {
            queueDataList = new LinkedList<>();
            queueDataList.add(queueData);
            this.topicQueueTable.put(topicName, queueDataList);
            log.info("new topic registered, {} {}", topicName, queueData);
        } else {

            // 这么麻烦，为啥不用 Set 呢？

            boolean addNewOne = true;
            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    if (qd.equals(queueData)) {
                        addNewOne = false;
                    } else {
                        log.info("topic changed, {} OLD: {} NEW: {}", topicName, qd, queueData);
                        it.remove();
                    }
                }
            }
            if (addNewOne) {
                queueDataList.add(queueData);
            }
        }
    }

    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return wipeWritePermOfBroker(brokerName);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("wipeWritePermOfBrokerByLock Exception", e);
        }

        return 0;
    }

    private int wipeWritePermOfBroker(final String brokerName) {
        int wipeTopicCnt = 0;
        for (Entry<String, List<QueueData>> entry : this.topicQueueTable.entrySet()) {
            List<QueueData> qdList = entry.getValue();
            for (QueueData qd : qdList) {
                if (qd.getBrokerName().equals(brokerName)) {
                    int perm = qd.getPerm();
                    perm &= ~PermName.PERM_WRITE;
                    qd.setPerm(perm);
                    wipeTopicCnt++;
                }
            }
        }
        return wipeTopicCnt;
    }

    public void unregisterBroker(final String clusterName, final String brokerAddr, final String brokerName, final long brokerId) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddr);
                log.info("unregisterBroker, remove from brokerLiveTable {}, {}", brokerLiveInfo != null ? "OK" : "Failed", brokerAddr);

                this.filterServerTable.remove(brokerAddr);

                // 因为一个 brokerName 下面可能存在多个 broker，如果该 brokerName 下到所有 broker 都剔除了，则把该 brokerName 相关的所有信息都剔除，此时 removeBrokerName = true
                boolean removeBrokerName = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    // 移除该 brokerId 地址
                    brokerData.getBrokerAddrs().remove(brokerId);

                    if (brokerData.getBrokerAddrs().isEmpty()/* 如果移除该 brokerId 之后该 brokerName 下面都broker地址为空了，则说明需要移除该 brokerName */) {
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}", brokerName);

                        removeBrokerName = true;
                    }
                }

                if (removeBrokerName) {
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}", removed ? "OK" : "Failed", brokerName);

                        if (nameSet.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}", clusterName);
                        }
                    }
                    this.removeTopicByBrokerName(brokerName);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("unregisterBroker Exception", e);
        }
    }

    private void removeTopicByBrokerName(final String brokerName) {
        Iterator<Entry<String, List<QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
        while (itMap.hasNext()) {
            Entry<String, List<QueueData>> entry = itMap.next();

            String topic = entry.getKey();
            List<QueueData> queueDataList = entry.getValue();
            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    log.info("removeTopicByBrokerName, remove one broker's topic {} {}", topic, qd);
                    it.remove();
                }
            }

            if (queueDataList.isEmpty()) {
                log.info("removeTopicByBrokerName, remove the topic all queue {}", topic);
                itMap.remove();
            }
        }
    }

    /**
     * 在namesrv中查询该主题topic的路由信息
     *
     * @param topic 主题
     * @return 该主题topic的路由信息
     */
    public TopicRouteData pickupTopicRouteData(final String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundQueueData = false;
        boolean foundBrokerData = false;
        Set<String> brokerNameSet = new HashSet<>();
        List<BrokerData> brokerDataList = new LinkedList<>();
        topicRouteData.setBrokerDatas(brokerDataList);

        HashMap<String, List<String>> filterServerMap = new HashMap<>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            try {
                this.lock.readLock().lockInterruptibly();
                List<QueueData> queueDataList = this.topicQueueTable.get(topic);
                if (queueDataList != null) {
                    topicRouteData.setQueueDatas(queueDataList);
                    foundQueueData = true;

                    for (QueueData qd : queueDataList) {
                        brokerNameSet.add(qd.getBrokerName());
                    }

                    for (String brokerName : brokerNameSet) {
                        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                        if (null != brokerData) {
                            @SuppressWarnings("unchecked")
                            BrokerData brokerDataClone = new BrokerData(brokerData.getCluster(), brokerData.getBrokerName(), (HashMap<Long, String>) brokerData.getBrokerAddrs().clone());
                            brokerDataList.add(brokerDataClone);
                            foundBrokerData = true;
                            for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                                List<String> filterServerList = this.filterServerTable.get(brokerAddr);
                                filterServerMap.put(brokerAddr, filterServerList);
                            }
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        }

        log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);

        if (foundBrokerData && foundQueueData) {
            return topicRouteData;
        }

        return null;
    }

    /**
     * 扫描不活跃\断线的Broker的原理很简单,仅仅是拿当前时间减去上一次检查的时间,如果时间超过了阀值就会提出对应Broker的Channel.
     */
    public void scanNotActiveBroker() {
        // HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
        Iterator<Entry<String/* brokerAddr */, BrokerLiveInfo/*存活情况*/>> it = this.brokerLiveTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String/* brokerAddr */, BrokerLiveInfo/*存活情况*/> next = it.next();

            // 该 broker 上次向 namesrv 心跳成功的事件！
            long last = next.getValue().getLastUpdateTimestamp();
            if ((last + BROKER_CHANNEL_EXPIRED_TIME/*120秒*/) < System.currentTimeMillis()/*该 broker 超过2分钟没发心跳给 namesrv，则认为出问题了*/) {
                BrokerLiveInfo brokerLiveInfo = next.getValue();
                Channel channel = brokerLiveInfo.getChannel();
                RemotingUtil.closeChannel(channel);
                it.remove();
                log.warn("The broker channel expired, {} {}ms", next.getKey(), BROKER_CHANNEL_EXPIRED_TIME);
                // 针对该 broker 的路由信息进行删除
                this.onChannelDestroy(next.getKey(), channel);
            }
        }
    }

    /**
     * 既然找到了一个已经宕机的 broker,自然这个 broker 的所有 路由信息都要移除掉，包括：
     * topicQueueTable
     * brokerAddrTable
     * clusterAddrTable
     * brokerLiveTable
     * 等
     *
     * 其实就是一个 CRUD
     *
     * @param remoteAddr 移除的 broker 地址
     * @param channel 可能存在的连接
     * @see RouteInfoManager#scanNotActiveBroker() 定时任务调用
     * @see BrokerHousekeepingService#channelDestroy(java.lang.String, io.netty.channel.Channel)
     */
    public void onChannelDestroy(String remoteAddr, Channel channel) {
        // 根据channel查询到的 broker 地址，如：127.0.0.1:10911
        String brokerAddrFound = null;
        if (channel != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    for (Entry<String/* brokerAddr，如 127.0.0.1:10911 在整个集群中是唯一的 */, BrokerLiveInfo/*封装了该broker最近心跳的时间,broker是否存活也依赖该时间*/> entry : this.brokerLiveTable.entrySet()) {
                        BrokerLiveInfo brokerLiveInfo/*封装了该broker最近心跳的时间,broker是否存活也依赖该时间*/ = entry.getValue();
                        if (brokerLiveInfo.getChannel() == channel) {
                            brokerAddrFound = entry.getKey(); // 127.0.0.1:10911
                            break;
                        }
                    }
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (null == brokerAddrFound) {
            brokerAddrFound = remoteAddr;
        } else {
            log.info("the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
        }
        /**
         * brokerAddrFound ：
         * 优先根据 channle 从存活broker映射表中匹配，匹配不到则使用 传入的 remoteAddr
         */

        if (brokerAddrFound != null && brokerAddrFound.length() > 0) {
            /**
             * 既然找到了一个已经宕机的 broker,自然这个 broker 的所有 路由信息都要移除掉，包括：
             * topicQueueTable
             * brokerAddrTable
             * clusterAddrTable
             * brokerLiveTable
             * 等
             */
            try {
                try {
                    this.lock.writeLock().lockInterruptibly();

                    // HashMap<String/* brokerAddr，如 127.0.0.1:10911 在整个集群中是唯一的 */, BrokerLiveInfo /*封装了该broker最近心跳的时间,broker是否存活也依赖该时间*/> brokerLiveTable;
                    // 从 broker 存活表中根据 broker 地址移除
                    this.brokerLiveTable.remove(brokerAddrFound);
                    this.filterServerTable.remove(brokerAddrFound);

                    // 该宕机 broker 的名称
                    String brokerNameFound = null;
                    // 是否需要移除该broker？只有当该 broker 下面的所有 master，slave节点都不在的时候才移除
                    boolean removeBrokerName = false;

                    // HashMap<String/* brokerName，如：broker-a */, BrokerData /* broker 信息,包括主从 broker，只要 brokerName 相关的 broker 都会封装在一个对象中，其实都是有配置决定 */> brokerAddrTable;
                    Iterator<Entry<String/* brokerName，如：broker-a */, BrokerData/* broker 信息,包括主从 broker，只要 brokerName 相关的 broker 都会封装在一个对象中，其实都是有配置决定 */>> itBrokerAddrTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerAddrTable.hasNext() && (null == brokerNameFound/*循环中找到了名称就可以跳出循环了，没必要进行无谓的循环*/)) {

                        Entry<String/* brokerName，如：broker-a */, BrokerData/*该brokerName相同的所有broker集合都在这个对象*/> brokerDataEntry = itBrokerAddrTable.next();

                        // 该brokerName相同的所有broker集合都在这个对象
                        BrokerData brokerData = brokerDataEntry.getValue();

                        // HashMap<Long/* brokerId,如 0为master，其他为 slave节点  */, String/* broker address 如 127.0.0.1:10911*/> brokerAddrs;
                        HashMap<Long/* brokerId,如 0为master，其他为 slave节点  */, String/* broker address 如 127.0.0.1:10911*/> brokerAddrs = brokerData.getBrokerAddrs();
                        Iterator<Entry<Long/* brokerId,如 0为master，其他为 slave节点  */, String/* broker address 如 127.0.0.1:10911*/>> it = brokerAddrs.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long/* brokerId,如 0为master，其他为 slave节点  */, String/* broker address 如 127.0.0.1:10911*/> entry = it.next();
                            Long brokerId = entry.getKey(); // brokerId,如 0为master，其他为 slave节点
                            String brokerAddr = entry.getValue(); // /* broker address 如 127.0.0.1:10911*/
                            if (brokerAddr.equals(brokerAddrFound/*要移除的 broker 地址*/)) {
                                // 找到了
                                brokerNameFound = brokerData.getBrokerName();
                                // 移除
                                it.remove();
                                log.info("remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed", brokerId, brokerAddr);
                                break;
                            }
                        }

                        if (brokerData.getBrokerAddrs().isEmpty()/*如果移除这个 broker 之后该 broker名称下面没有其他 broker 了*/) {
                            // 移除这个 broker 之后该 broker名称下面没有其他 broker 了，则这个 broker 名称对应的所有数据都要初一
                            removeBrokerName = true;
                            itBrokerAddrTable.remove();
                            log.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed", brokerData.getBrokerName());
                        }
                    }

                    if (brokerNameFound/*宕机的broker对应的 brokerName*/ != null && removeBrokerName/*该brokerName 下面已经没有其他 broker 了*/) {

                        // HashMap<String/* clusterName 集群名称 */, Set<String/* brokerName */> /*某个集群下面的所有 broker 名称集合*/> clusterAddrTable;
                        Iterator<Entry<String/* clusterName 集群名称 */, Set<String/* brokerName */>/*某个集群下面的所有 broker 名称集合*/>> it = this.clusterAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            // 循环的目的就是要把 brokerNameFound 相关的数据移除

                            Entry<String, Set<String>> entry = it.next();
                            String clusterName = entry.getKey();
                            Set<String> brokerNames = entry.getValue();
                            boolean removed = brokerNames.remove(brokerNameFound);
                            if (removed) {
                                log.info("remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed", brokerNameFound, clusterName);
                                if (brokerNames.isEmpty()) {
                                    log.info("remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster", clusterName);

                                    // 如果该集群下面没有其他 brokerName了，则这个集群都移除
                                    it.remove();
                                }

                                break;
                            }
                        }
                    }

                    if (removeBrokerName) {
                        // HashMap<String/* topic */, List<QueueData> /*该主题下面的各个队列的属性*/> topicQueueTable;
                        Iterator<Entry<String/* topic */, List<QueueData>/*该主题下面的各个队列的属性*/>> itTopicQueueTable = this.topicQueueTable.entrySet().iterator();
                        while (itTopicQueueTable.hasNext()) {
                            Entry<String/* topic */, List<QueueData>/*该主题下面的各个队列的属性*/> entry = itTopicQueueTable.next();
                            String topic = entry.getKey();
                            // 该主题下面的队列信息
                            List<QueueData> queueDataList = entry.getValue();

                            Iterator<QueueData> itQueueData = queueDataList.iterator();
                            while (itQueueData.hasNext()) {
                                QueueData queueData = itQueueData.next();
                                if (queueData.getBrokerName().equals(brokerNameFound)) {
                                    itQueueData.remove();
                                    log.info("remove topic[{} {}], from topicQueueTable, because channel destroyed", topic, queueData);
                                }
                            }

                            if (queueDataList.isEmpty()) {
                                itTopicQueueTable.remove();
                                log.info("remove topic[{}] all queue, from topicQueueTable, because channel destroyed", topic);
                            }
                        }
                    }
                } finally {
                    this.lock.writeLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }
    }

    public void printAllPeriodically() {
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                log.info("--------------------------------------------------------");
                {
                    log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                    for (Entry<String, List<QueueData>> next : this.topicQueueTable.entrySet()) {
                        log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                    for (Entry<String, BrokerData> next : this.brokerAddrTable.entrySet()) {
                        log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                    for (Entry<String, BrokerLiveInfo> next : this.brokerLiveTable.entrySet()) {
                        log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
                    for (Entry<String, Set<String>> next : this.clusterAddrTable.entrySet()) {
                        log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("printAllPeriodically Exception", e);
        }
    }

    public byte[] getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
                    topicList.getTopicList().add(entry.getKey());
                    topicList.getTopicList().addAll(entry.getValue());
                }

                if (!brokerAddrTable.isEmpty()) {
                    for (String s : brokerAddrTable.keySet()) {
                        BrokerData bd = brokerAddrTable.get(s);
                        HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
                        if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
                            Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                            topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
                            break;
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
                for (String brokerName : brokerNameSet) {
                    for (Entry<String, List<QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                        String topic = topicEntry.getKey();
                        List<QueueData> queueDatas = topicEntry.getValue();
                        for (QueueData queueData : queueDatas) {
                            if (brokerName.equals(queueData.getBrokerName())) {
                                topicList.getTopicList().add(topic);
                                break;
                            }
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getUnitTopics() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                for (Entry<String, List<QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0 && TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getHasUnitSubTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                for (Entry<String, List<QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0 && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getHasUnitSubUnUnitTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                for (Entry<String, List<QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0 && !TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSynFlag()) && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }
}

@AllArgsConstructor
@ToString
class BrokerLiveInfo {

    /**
     * 上次心态时间，通过这个判断broker是否存活，如果是两分钟之前，则认为对应的 broker 节点掉线了
     */
    @Setter
    @Getter
    private long lastUpdateTimestamp;

    /**
     * 版本
     */
    @Setter
    @Getter
    private DataVersion dataVersion;

    /**
     * 通信通道
     */
    @Setter
    @Getter
    private Channel channel;

    /**
     * 备份节点地址
     */
    @Setter
    @Getter
    private String haServerAddr;
}

package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.broker.util.PositiveAtomicCounter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 该 broker 下的生产者管理
 */
@NoArgsConstructor
public class ProducerManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;

    private static final int GET_AVALIABLE_CHANNEL_RETRY_COUNT = 3;

    @Getter
    private final ConcurrentHashMap<String /* group name */, ConcurrentHashMap<Channel/*生产者与broker的长连接*/, ClientChannelInfo/*生产者与broker的长连接信息*/>> groupChannelTable = new ConcurrentHashMap<>();

    // 各个生产者实例id与连接的映射表
    private final ConcurrentHashMap<String/*clientId*/, Channel/*生产者与broker的长连接*/> clientChannelTable = new ConcurrentHashMap<>();

    private final PositiveAtomicCounter positiveAtomicCounter = new PositiveAtomicCounter();

    public void scanNotActiveChannel() {
        for (final Map.Entry<String /* group name */, ConcurrentHashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable.entrySet()) {
            final String group = entry.getKey();
            final ConcurrentHashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

            Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Channel, ClientChannelInfo> item = it.next();
                // final Integer id = item.getKey();
                final ClientChannelInfo info = item.getValue();

                // 没有活跃的时间
                long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    // 超过120s认为是不活跃，移除
                    it.remove();
                    clientChannelTable.remove(info.getClientId());
                    log.warn("SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}", RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                    RemotingUtil.closeChannel(info.getChannel());
                }
            }
        }
    }

    public synchronized void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel == null) {
            return;
        }
        for (final Map.Entry<String  /* group name */, ConcurrentHashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable.entrySet()) {
            final String group = entry.getKey();
            final ConcurrentHashMap<Channel, ClientChannelInfo> clientChannelInfoTable = entry.getValue();
            final ClientChannelInfo clientChannelInfo = clientChannelInfoTable.remove(channel);
            if (clientChannelInfo != null) {
                clientChannelTable.remove(clientChannelInfo.getClientId());
                log.info("NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}", clientChannelInfo.toString(), remoteAddr, group);
            }

        }
    }

    public synchronized void registerProducer(final String group, final ClientChannelInfo clientChannelInfo/*生产者与broker的长连接信息*/) {
        ClientChannelInfo clientChannelInfoFound;

        // 该生产者组下面的所有连接映射表
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
        if (null == channelTable) {
            channelTable = new ConcurrentHashMap<>();
            this.groupChannelTable.put(group, channelTable);
        }

        clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
        if (null == clientChannelInfoFound) {
            channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
            clientChannelTable.put(clientChannelInfo.getClientId(), clientChannelInfo.getChannel());
            log.info("new producer connected, group: {} channel: {}", group, clientChannelInfo.toString());
        }

        if (clientChannelInfoFound != null) {
            // 更新成最新时刻
            clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    public synchronized void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        // 该生产者组下面的所有连接映射表
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
        if (null != channelTable && !channelTable.isEmpty()) {
            // 该组下面有连接存在，则继续

            ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
            clientChannelTable.remove(clientChannelInfo.getClientId());
            if (old != null) {
                log.info("unregister a producer[{}] from groupChannelTable {}", group, clientChannelInfo.toString());
            }

            if (channelTable.isEmpty()) {
                this.groupChannelTable.remove(group);
                log.info("unregister a producer group[{}] from groupChannelTable", group);
            }
        }
    }

    public Channel getAvaliableChannel(String groupId/*groupName*/) {
        if (groupId == null) {
            return null;
        }
        List<Channel> channelList;

        // 根据group 查询该生产者组下的所有连接
        ConcurrentHashMap<Channel, ClientChannelInfo> channelClientChannelInfoHashMap = groupChannelTable.get(groupId);
        if (channelClientChannelInfoHashMap != null) {
            channelList = new ArrayList<>(channelClientChannelInfoHashMap.keySet());
        } else {
            log.warn("Check transaction failed, channel table is empty. groupId={}", groupId);
            return null;
        }

        int size = channelList.size();
        if (0 == size) {
            log.warn("Channel list is empty. groupId={}", groupId);
            return null;
        }

        Channel lastActiveChannel = null;

        int index = positiveAtomicCounter.incrementAndGet() % size;
        Channel channel = channelList.get(index);
        int count = 0;

        // 找到一个活跃的并且可写的连接
        boolean isOk = channel.isActive() && channel.isWritable();
        while (count++ < GET_AVALIABLE_CHANNEL_RETRY_COUNT/*3*/) {
            if (isOk) {
                return channel;
            }
            if (channel.isActive()) {
                lastActiveChannel = channel;
            }
            index = (++index) % size;
            channel = channelList.get(index);
            isOk = channel.isActive() && channel.isWritable();
        }

        return lastActiveChannel;
    }

    public Channel findChannel(String clientId) {
        // ConcurrentHashMap<String/*clientId*/, Channel/*生产者与broker的长连接*/> clientChannelTable = new ConcurrentHashMap<>();
        return clientChannelTable.get(clientId);
    }
}

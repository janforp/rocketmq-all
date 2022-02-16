package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

@ToString
@Getter
public class ClientChannelInfo {

    private final Channel channel;

    // 客户端id
    private final String clientId;

    private final LanguageCode language;

    private final int version;

    /**
     * 上一次心跳的时间
     */
    @Setter
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ClientChannelInfo(Channel channel) {
        this(channel, null, null, 0);
    }

    public ClientChannelInfo(Channel channel, String clientId, LanguageCode language, int version) {
        this.channel = channel;
        this.clientId = clientId;
        this.language = language;
        this.version = version;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        result = prime * result + ((clientId == null) ? 0 : clientId.hashCode());
        result = prime * result + ((language == null) ? 0 : language.hashCode());
        result = prime * result + (int) (lastUpdateTimestamp ^ (lastUpdateTimestamp >>> 32));
        result = prime * result + version;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ClientChannelInfo other = (ClientChannelInfo) obj;
        if (channel == null) {
            return other.channel == null;
        } else {
            return this.channel == other.channel;
        }
    }
}
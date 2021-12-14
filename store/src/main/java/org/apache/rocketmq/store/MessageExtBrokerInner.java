package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.MessageExt;

public class MessageExtBrokerInner extends MessageExt {

    private static final long serialVersionUID = 7256001576878700634L;

    @Getter
    @Setter
    private String propertiesString;

    @Getter
    @Setter
    private long tagsCode;

    public static long tagsString2tagsCode(final TopicFilterType filter, final String tags) {
        if (null == tags || tags.length() == 0) {
            return 0;
        }

        return tags.hashCode();
    }

    public static long tagsString2tagsCode(final String tags) {
        return tagsString2tagsCode(null, tags);
    }
}

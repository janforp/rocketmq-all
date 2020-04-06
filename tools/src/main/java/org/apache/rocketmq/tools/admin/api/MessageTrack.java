package org.apache.rocketmq.tools.admin.api;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MessageTrack {

    private String consumerGroup;

    private TrackType trackType;

    private String exceptionDesc;

    @Override
    public String toString() {
        return "MessageTrack [consumerGroup=" + consumerGroup + ", trackType=" + trackType
                + ", exceptionDesc=" + exceptionDesc + "]";
    }
}

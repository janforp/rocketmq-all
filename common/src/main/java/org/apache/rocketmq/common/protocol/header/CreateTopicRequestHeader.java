package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Getter
@Setter
public class CreateTopicRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String topic;

    @CFNotNull
    private String defaultTopic;

    @CFNotNull
    private Integer readQueueNums;

    @CFNotNull
    private Integer writeQueueNums;

    @CFNotNull
    private Integer perm;

    @CFNotNull
    private String topicFilterType;

    private Integer topicSysFlag;

    @CFNotNull
    private Boolean order = false;

    @Override
    public void checkFields() throws RemotingCommandException {
        try {
            TopicFilterType.valueOf(this.topicFilterType);
        } catch (Exception e) {
            throw new RemotingCommandException("topicFilterType = [" + topicFilterType + "] value invalid", e);
        }
    }

    public TopicFilterType getTopicFilterTypeEnum() {
        return TopicFilterType.valueOf(this.topicFilterType);
    }

}

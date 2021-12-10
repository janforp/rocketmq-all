package org.apache.rocketmq.common.protocol.heartbeat;

public class ProducerData {

    // 生产者组名称
    private String groupName;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    @Override
    public String toString() {
        return "ProducerData [groupName=" + groupName + "]";
    }
}

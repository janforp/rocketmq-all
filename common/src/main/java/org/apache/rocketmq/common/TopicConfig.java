package org.apache.rocketmq.common;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.rocketmq.common.constant.PermName;

@NoArgsConstructor
public class TopicConfig {

    private static final String SEPARATOR = " ";

    public static int defaultReadQueueNums = 16;

    public static int defaultWriteQueueNums = 16;

    // 主题
    @Getter
    @Setter
    private String topicName;

    @Getter
    @Setter
    private int readQueueNums = defaultReadQueueNums;

    @Getter
    @Setter
    private int writeQueueNums = defaultWriteQueueNums;

    @Getter
    @Setter
    private int perm = PermName.PERM_READ | PermName.PERM_WRITE;

    @Getter
    @Setter
    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;

    @Getter
    @Setter
    private int topicSysFlag = 0;

    @Getter
    @Setter
    private boolean order = false;

    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }

    public String encode() {
        return this.topicName
                + SEPARATOR
                + this.readQueueNums
                + SEPARATOR
                + this.writeQueueNums
                + SEPARATOR
                + this.perm
                + SEPARATOR
                + this.topicFilterType;
    }

    public boolean decode(final String in) {
        String[] strs = in.split(SEPARATOR);
        if (strs != null && strs.length == 5) {
            this.topicName = strs[0];

            this.readQueueNums = Integer.parseInt(strs[1]);

            this.writeQueueNums = Integer.parseInt(strs[2]);

            this.perm = Integer.parseInt(strs[3]);

            this.topicFilterType = TopicFilterType.valueOf(strs[4]);

            return true;
        }

        return false;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final TopicConfig that = (TopicConfig) o;

        if (readQueueNums != that.readQueueNums) {
            return false;
        }
        if (writeQueueNums != that.writeQueueNums) {
            return false;
        }
        if (perm != that.perm) {
            return false;
        }
        if (topicSysFlag != that.topicSysFlag) {
            return false;
        }
        if (order != that.order) {
            return false;
        }
        if (topicName != null ? !topicName.equals(that.topicName) : that.topicName != null) {
            return false;
        }
        return topicFilterType == that.topicFilterType;

    }

    @Override
    public int hashCode() {
        int result = topicName != null ? topicName.hashCode() : 0;
        result = 31 * result + readQueueNums;
        result = 31 * result + writeQueueNums;
        result = 31 * result + perm;
        result = 31 * result + (topicFilterType != null ? topicFilterType.hashCode() : 0);
        result = 31 * result + topicSysFlag;
        result = 31 * result + (order ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TopicConfig [topicName=" + topicName + ", readQueueNums=" + readQueueNums
                + ", writeQueueNums=" + writeQueueNums + ", perm=" + PermName.perm2String(perm)
                + ", topicFilterType=" + topicFilterType + ", topicSysFlag=" + topicSysFlag + ", order="
                + order + "]";
    }
}

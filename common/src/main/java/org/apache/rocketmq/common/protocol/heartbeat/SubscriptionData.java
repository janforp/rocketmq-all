package org.apache.rocketmq.common.protocol.heartbeat;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;

import java.util.HashSet;
import java.util.Set;

@ToString
public class SubscriptionData implements Comparable<SubscriptionData> {

    public final static String SUB_ALL = "*";

    @Getter
    @Setter
    private boolean classFilterMode = false;

    @Getter
    @Setter
    private String topic;

    // 过滤表达式，如 aa bb cc
    @Getter
    @Setter
    private String subString;

    /**
     * 人员： [aa,bb,cc]
     * @see FilterAPI#buildSubscriptionData(java.lang.String, java.lang.String, java.lang.String)
     */
    @Getter
    @Setter
    private Set<String> tagsSet = new HashSet<String>();

    /**
     * 例如：[aa.hashcode,bb.hashcode,cc.hashcode]
     * @see FilterAPI#buildSubscriptionData(java.lang.String, java.lang.String, java.lang.String)
     */
    @Getter
    @Setter
    private Set<Integer> codeSet = new HashSet<Integer>();

    @Getter
    @Setter
    private long subVersion = System.currentTimeMillis();

    @Getter
    @Setter
    private String expressionType = ExpressionType.TAG;

    @Getter
    @Setter
    @JSONField(serialize = false)
    private String filterClassSource;

    public SubscriptionData() {

    }

    public SubscriptionData(String topic, String subString) {
        super();
        this.topic = topic;
        this.subString = subString;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (classFilterMode ? 1231 : 1237);
        result = prime * result + ((codeSet == null) ? 0 : codeSet.hashCode());
        result = prime * result + ((subString == null) ? 0 : subString.hashCode());
        result = prime * result + ((tagsSet == null) ? 0 : tagsSet.hashCode());
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + ((expressionType == null) ? 0 : expressionType.hashCode());
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
        SubscriptionData other = (SubscriptionData) obj;
        if (classFilterMode != other.classFilterMode) {
            return false;
        }
        if (codeSet == null) {
            if (other.codeSet != null) {
                return false;
            }
        } else if (!codeSet.equals(other.codeSet)) {
            return false;
        }
        if (subString == null) {
            if (other.subString != null) {
                return false;
            }
        } else if (!subString.equals(other.subString)) {
            return false;
        }
        if (subVersion != other.subVersion) {
            return false;
        }
        if (tagsSet == null) {
            if (other.tagsSet != null) {
                return false;
            }
        } else if (!tagsSet.equals(other.tagsSet)) {
            return false;
        }
        if (topic == null) {
            if (other.topic != null) {
                return false;
            }
        } else if (!topic.equals(other.topic)) {
            return false;
        }
        if (expressionType == null) {
            return other.expressionType == null;
        } else
            return expressionType.equals(other.expressionType);
    }

    @Override
    public int compareTo(SubscriptionData other) {
        String thisValue = this.topic + "@" + this.subString;
        String otherValue = other.topic + "@" + other.subString;
        return thisValue.compareTo(otherValue);
    }
}

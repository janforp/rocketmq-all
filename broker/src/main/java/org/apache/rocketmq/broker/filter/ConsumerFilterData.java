package org.apache.rocketmq.broker.filter;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.rocketmq.filter.expression.Expression;
import org.apache.rocketmq.filter.util.BloomFilterData;

import java.util.Collections;

/**
 * Filter data of consumer.
 */
@Getter
@Setter
public class ConsumerFilterData {

    private String consumerGroup;

    private String topic;

    private String expression;

    private String expressionType;

    private transient Expression compiledExpression;

    private long bornTime;

    private long deadTime = 0;

    private BloomFilterData bloomFilterData;

    private long clientVersion;

    public boolean isDead() {
        return this.deadTime >= this.bornTime;
    }

    public long howLongAfterDeath() {
        if (isDead()) {
            return System.currentTimeMillis() - getDeadTime();
        }
        return -1;
    }

    /**
     * Check this filter data has been used to calculate bit map when msg was stored in server.
     */
    public boolean isMsgInLive(long msgStoreTime) {
        return msgStoreTime > getBornTime();
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o, Collections.<String>emptyList());
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, Collections.<String>emptyList());
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
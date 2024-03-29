package org.apache.rocketmq.client.consumer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.common.filter.ExpressionType;

/**
 * Message selector: select message at server.
 * <p>
 * Now, support:
 * <li>Tag: {@link org.apache.rocketmq.common.filter.ExpressionType#TAG}
 * </li>
 * <li>SQL92: {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}
 * </li>
 * </p>
 */
@AllArgsConstructor
public class MessageSelector {

    /**
     * @see org.apache.rocketmq.common.filter.ExpressionType
     */
    @Getter
    private final String type;

    /**
     * expression content.
     */
    @Getter
    private final String expression;

    /**
     * 静态工厂方法
     *
     * Use SLQ92 to select message.
     *
     * @param sql if null or empty, will be treated as select all message.
     */
    public static MessageSelector bySql(String sql) {
        return new MessageSelector(ExpressionType.SQL92, sql);
    }

    /**
     * 静态工厂方法
     *
     * Use tag to select message.
     *
     * @param tag if null or empty or "*", will be treated as select all message.
     */
    public static MessageSelector byTag(String tag) {
        return new MessageSelector(ExpressionType.TAG, tag);
    }
}
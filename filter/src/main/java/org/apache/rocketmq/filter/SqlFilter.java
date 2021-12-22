package org.apache.rocketmq.filter;

import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.filter.expression.Expression;
import org.apache.rocketmq.filter.expression.MQFilterException;
import org.apache.rocketmq.filter.parser.SelectorParser;

/**
 * SQL92 Filter, just a wrapper of {@link org.apache.rocketmq.filter.parser.SelectorParser}.
 * <p/>
 * <p>
 * Do not use this filter directly.Use {@link FilterFactory#get} to select a filter.
 * </p>
 */
public class SqlFilter implements FilterSpi {

    @Override
    public Expression compile(final String expr) throws MQFilterException {
        return SelectorParser.parse(expr);
    }

    @Override
    public String ofType() {
        return ExpressionType.SQL92;
    }
}

package org.apache.rocketmq.filter;

import org.apache.rocketmq.filter.expression.Expression;
import org.apache.rocketmq.filter.expression.MQFilterException;

/**
 * Filter spi interface.
 */
public interface FilterSpi {

    /**
     * Compile.
     */
    Expression compile(final String expr) throws MQFilterException;

    /**
     * Which type.
     */
    String ofType();
}

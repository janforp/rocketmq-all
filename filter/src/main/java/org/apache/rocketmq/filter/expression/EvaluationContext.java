package org.apache.rocketmq.filter.expression;

import java.util.Map;

/**
 * Context of evaluate expression.
 *
 * Compare to org.apache.activemq.filter.MessageEvaluationContext, this is just an interface.
 */
public interface EvaluationContext {

    /**
     * Get value by name from context
     */
    Object get(String name);

    /**
     * Context variables.
     */
    Map<String, Object> keyValues();
}
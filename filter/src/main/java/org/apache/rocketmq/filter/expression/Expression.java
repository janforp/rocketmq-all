package org.apache.rocketmq.filter.expression;

/**
 * Interface of expression.
 * <p>
 * This class was taken from ActiveMQ org.apache.activemq.filter.Expression,
 * but the parameter is changed to an interface.
 * </p>
 *
 * @see org.apache.rocketmq.filter.expression.EvaluationContext
 */
public interface Expression {

    /**
     * Calculate express result with context.
     *
     * @param context context of evaluation
     * @return the value of this expression
     */
    Object evaluate(EvaluationContext context) throws Exception;
}
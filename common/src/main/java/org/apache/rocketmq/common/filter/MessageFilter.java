package org.apache.rocketmq.common.filter;

import org.apache.rocketmq.common.message.MessageExt;

public interface MessageFilter {

    boolean match(final MessageExt msg, final FilterContext context);
}

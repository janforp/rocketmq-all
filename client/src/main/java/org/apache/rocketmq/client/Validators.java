package org.apache.rocketmq.client;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.ResponseCode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Common Validator
 */
public class Validators {

    public static final String VALID_PATTERN_STR = "^[%|a-zA-Z0-9_-]+$";

    public static final Pattern PATTERN = Pattern.compile(VALID_PATTERN_STR);

    public static final int CHARACTER_MAX_LENGTH = 255;

    /**
     * topic最长127位
     */
    public static final int TOPIC_MAX_LENGTH = 127;

    /**
     * @return The resulting {@code String}
     */
    public static String getGroupWithRegularExpression(String origin, String patternStr) {
        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(origin);
        while (matcher.find()) {
            return matcher.group(0);
        }
        return null;
    }

    /**
     * Validate group
     */
    public static void checkGroup(String group) throws MQClientException {
        if (UtilAll.isBlank(group)) {
            throw new MQClientException("the specified group is blank", null);
        }

        if (group.length() > CHARACTER_MAX_LENGTH/*255*/) {
            throw new MQClientException("the specified group is longer than group max length 255.", null);
        }

        if (!regularExpressionMatcher(group, PATTERN)) {
            throw new MQClientException(String.format(
                    "the specified group[%s] contains illegal characters, allowing only %s", group,
                    VALID_PATTERN_STR), null);
        }

    }

    /**
     * @return <tt>true</tt> if, and only if, the entire origin sequence matches this matcher's pattern
     */
    public static boolean regularExpressionMatcher(String origin, Pattern pattern) {
        if (pattern == null) {
            return true;
        }
        Matcher matcher = pattern.matcher(origin);
        return matcher.matches();
    }

    /**
     * 不能空，topic 必须合法，body不能空，body 长度不能超过 4M
     */
    public static void checkMessage(Message msg, DefaultMQProducer defaultMQProducer) throws MQClientException {
        if (null == msg) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message is null");
        }
        // topic
        Validators.checkTopic(msg.getTopic());

        // body
        if (null == msg.getBody()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body is null");
        }

        if (0 == msg.getBody().length) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body length is zero");
        }

        if (msg.getBody().length > defaultMQProducer.getMaxMessageSize()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body size over max value, MAX: " + defaultMQProducer.getMaxMessageSize());
        }
    }

    /**
     * 不能空，最长127，只允许 ^[%|a-zA-Z0-9_-]+$，不能是保留更关键词
     */
    public static void checkTopic(String topic) throws MQClientException {
        if (UtilAll.isBlank(topic)) {
            throw new MQClientException("The specified topic is blank", null);
        }

        if (!regularExpressionMatcher(topic, PATTERN)) {
            throw new MQClientException(String.format("The specified topic[%s] contains illegal characters, allowing only %s", topic, VALID_PATTERN_STR), null);
        }

        if (topic.length() > TOPIC_MAX_LENGTH) {
            throw new MQClientException(String.format("The specified topic is longer than topic max length %d.", TOPIC_MAX_LENGTH), null);
        }

        //whether the same with system reserved keyword：TBW102，保留关键词
        if (topic.equals(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
            throw new MQClientException(String.format("The topic[%s] is conflict with AUTO_CREATE_TOPIC_KEY_TOPIC.", topic), null);
        }
    }
}

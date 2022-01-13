package org.apache.rocketmq.common;

import lombok.Data;

import java.util.List;

@Data
public class PlainAccessConfig {

    private String accessKey;

    private String secretKey;

    private String whiteRemoteAddress;

    private boolean admin;

    private String defaultTopicPerm;

    private String defaultGroupPerm;

    private List<String> topicPerms;

    private List<String> groupPerms;
}
package org.apache.rocketmq.client;

/**
 * Used for set access channel, if need migrate the rocketmq service to cloud, it is We recommend set the value with
 * "CLOUD". otherwise set with "LOCAL", especially used the message trace feature.
 */
public enum AccessChannel {
    /**
     * Means connect to private IDC cluster.
     */
    LOCAL,

    /**
     * Means connect to Cloud service.
     */
    CLOUD,
}

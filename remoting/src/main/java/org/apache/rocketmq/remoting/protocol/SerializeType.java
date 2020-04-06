package org.apache.rocketmq.remoting.protocol;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum SerializeType {
    JSON((byte) 0),
    ROCKETMQ((byte) 1);

    @Getter
    private byte code;

    public static SerializeType valueOf(byte code) {
        for (SerializeType serializeType : SerializeType.values()) {
            if (serializeType.getCode() == code) {
                return serializeType;
            }
        }
        return null;
    }
}

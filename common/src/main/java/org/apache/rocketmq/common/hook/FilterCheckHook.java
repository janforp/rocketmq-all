package org.apache.rocketmq.common.hook;

import java.nio.ByteBuffer;

public interface FilterCheckHook {

    public String hookName();

    public boolean isFilterMatched(final boolean isUnitMode, final ByteBuffer byteBuffer);
}

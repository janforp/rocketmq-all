package org.apache.rocketmq.store;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.nio.ByteBuffer;

@AllArgsConstructor
public class SelectMappedBufferResult {

    @Getter
    private final long startOffset;

    // 切片
    @Getter
    private final ByteBuffer byteBuffer;

    @Getter
    private int size;

    // 切片归属的文件
    private MappedFile mappedFile;

    public void setSize(final int s) {
        this.size = s;
        this.byteBuffer.limit(this.size);
    }

    public synchronized void release() {
        if (this.mappedFile != null) {
            this.mappedFile.release();
            this.mappedFile = null;
        }
    }
}

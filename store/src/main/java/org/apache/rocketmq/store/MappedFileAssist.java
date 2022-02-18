package org.apache.rocketmq.store;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * 0.....落盘数据.....flushedPosition.....脏页数据.....wrotePosition.......空闲部分.....文件结尾
 *
 * commitLog 顺序写的文件
 * consumerQueue
 * indexFile
 *
 * 系统调用：
 * mmap 零拷贝
 */
@SuppressWarnings("all")
public class MappedFileAssist {

    /**
     * TODO
     * 清理文件，释放资源
     */
    public static void clean(final ByteBuffer buffer/*释放这个资源的内存*/) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) {
            return;
        }

        ByteBuffer viewed = viewed(buffer);

        Object cleaner = invoke(viewed, "cleaner");

        invoke(cleaner, "clean");
    }

    // TODO
    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                Method method = method(target, methodName, args);
                method.setAccessible(true);
                return method.invoke(target);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args) throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    // TODO
    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (Method method : methods) {
            if (method.getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null) {
            return buffer;
        } else {
            return viewed(viewedBuffer);
        }
    }
}

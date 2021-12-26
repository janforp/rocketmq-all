package org.apache.rocketmq.common.sysflag;

public class PullSysFlag {

    // 0    0   0   0   0   0   0   1
    private final static int FLAG_COMMIT_OFFSET = 0x1;

    // 0    0   0   0   0   0   1   0
    private final static int FLAG_SUSPEND = 0x1 << 1;

    // 0    0   0   0   0   1   0   0
    private final static int FLAG_SUBSCRIPTION = 0x1 << 2;

    // 0    0   0   0   1   0   0   0
    private final static int FLAG_CLASS_FILTER = 0x1 << 3;

    // 0    0   0   1   0   0   0   0
    private final static int FLAG_LITE_PULL_MESSAGE = 0x1 << 4;

    public static void main(String[] args) {
        System.out.println(Integer.toBinaryString(FLAG_COMMIT_OFFSET));
        System.out.println(Integer.toBinaryString(FLAG_SUSPEND));
        System.out.println(Integer.toBinaryString(FLAG_SUBSCRIPTION));
        System.out.println(Integer.toBinaryString(FLAG_CLASS_FILTER));
        System.out.println(Integer.toBinaryString(FLAG_LITE_PULL_MESSAGE));

        System.out.println(Integer.toBinaryString(~FLAG_COMMIT_OFFSET));
    }

    /**
     * sysFlag bit 位表示
     *
     * 0    0   0   0   0   0   1   1
     *
     * 前四个bit没用
     * 第五个bit 表示是否位类过滤
     * 第六个bit 表示拉消息请求，是否包含消费者本地该topic的订阅信息
     * 第七个bit 是否允许服务器端长轮询，默认允许
     * 第八个bit 拉消息时，是否提交本地该队列的消费进度
     *
     * @param commitOffset
     * @param suspend
     * @param subscription
     * @param classFilter
     * @return
     */
    public static int buildSysFlag(final boolean commitOffset, final boolean suspend, final boolean subscription, final boolean classFilter) {
        int flag = 0;

        // 是否提交本地的offset
        if (commitOffset) {
            /*
             * 0    0   0   0   0   0   0   0
             * 0    0   0   0   0   0   0   1
             *
             *                              ｜
             *
             * 0    0   0   0   0   0   0   1
             */

            flag |= FLAG_COMMIT_OFFSET;
        }

        // 是否服务器长轮询？一般允许
        if (suspend) {
            /*
             * 0    0   0   0   0   0   0   1
             * 0    0   0   0   0   0   1   0
             *
             *                              ｜
             *
             * 0    0   0   0   0   0   1   1
             */
            flag |= FLAG_SUSPEND;
        }

        // 拉消息的时候，是否把消费者本地的该主题的订阅信息带过去？一般不需要
        if (subscription) {
            /*
             * 0    0   0   0   0   0   1   1
             * 0    0   0   0   0   1   0   0
             *
             *                              ｜
             *
             * 0    0   0   0   0   1   1   1
             */
            flag |= FLAG_SUBSCRIPTION;
        }

        // 是否为类过滤，一般为 TAG
        if (classFilter) {
            /*
             * 0    0   0   0   0   1   1   1
             * 0    0   0   0   1   0   0   0
             *
             *                              ｜
             *
             * 0    0   0   0   1   1   1   1
             */
            flag |= FLAG_CLASS_FILTER;
        }

        // 如果全都是 true，则 flag 为：0    0   0   0   1   1   1   1
        return flag;
    }

    public static int buildSysFlag(final boolean commitOffset, final boolean suspend, final boolean subscription, final boolean classFilter, final boolean litePull) {
        int flag = buildSysFlag(commitOffset, suspend, subscription, classFilter);

        if (litePull) {
            flag |= FLAG_LITE_PULL_MESSAGE;
        }

        return flag;
    }

    public static int clearCommitOffsetFlag(final int sysFlag) {
        return sysFlag & (~FLAG_COMMIT_OFFSET /* 11111111111111111111111111111110 */);
    }

    public static boolean hasCommitOffsetFlag(final int sysFlag) {
        return (sysFlag & FLAG_COMMIT_OFFSET) == FLAG_COMMIT_OFFSET;
    }

    public static boolean hasSuspendFlag(final int sysFlag) {
        return (sysFlag & FLAG_SUSPEND) == FLAG_SUSPEND;
    }

    public static boolean hasSubscriptionFlag(final int sysFlag) {
        return (sysFlag & FLAG_SUBSCRIPTION) == FLAG_SUBSCRIPTION;
    }

    public static boolean hasClassFilterFlag(final int sysFlag) {
        return (sysFlag & FLAG_CLASS_FILTER) == FLAG_CLASS_FILTER;
    }

    public static boolean hasLitePullFlag(final int sysFlag) {
        return (sysFlag & FLAG_LITE_PULL_MESSAGE) == FLAG_LITE_PULL_MESSAGE;
    }
}

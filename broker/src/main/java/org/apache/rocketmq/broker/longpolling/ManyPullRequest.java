package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;

public class ManyPullRequest {

    /**
     * 正常情况下只有一个元素，因为正常情况下一个queue只会分配给一个消费者
     *
     * 那么什么时候会有多个元素呢？当使用的是广播模式的时候就可能有多个，当使用集群的时候，当该集群加了或者减了一个节点，rbl重新平衡之后，原节点跟新节点都发送了请求，但是最终还是只有一个
     */
    private final ArrayList<PullRequest> pullRequestList = new ArrayList<>();

    public synchronized void addPullRequest(final PullRequest pullRequest) {
        this.pullRequestList.add(pullRequest);
    }

    public synchronized void addPullRequest(final List<PullRequest> many) {
        this.pullRequestList.addAll(many);
    }

    public synchronized List<PullRequest> cloneListAndClear() {
        if (!this.pullRequestList.isEmpty()) {
            @SuppressWarnings("unchecked")
            List<PullRequest> result = (ArrayList<PullRequest>) this.pullRequestList.clone();
            this.pullRequestList.clear();
            return result;
        }

        return null;
    }
}
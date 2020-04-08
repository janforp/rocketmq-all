package org.apache.rocketmq.client.common;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ThreadLocalIndexTest {

    @Test
    public void testGetAndIncrement() throws Exception {

        ThreadLocalIndex localIndex = new ThreadLocalIndex();

        int initialVal = localIndex.getAndIncrement();

        int andIncrement = localIndex.getAndIncrement();

        int result = initialVal + 1;

        assertThat(andIncrement).isEqualTo(result);
    }
}
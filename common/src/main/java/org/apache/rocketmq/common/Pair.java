package org.apache.rocketmq.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class Pair<T1, T2> {

    @Setter
    @Getter
    private T1 object1;

    @Setter
    @Getter
    private T2 object2;

}

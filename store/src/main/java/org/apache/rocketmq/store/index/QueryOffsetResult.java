package org.apache.rocketmq.store.index;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class QueryOffsetResult {

    private final List<Long> phyOffsets;

    private final long indexLastUpdateTimestamp;

    private final long indexLastUpdatePhyoffset;
}
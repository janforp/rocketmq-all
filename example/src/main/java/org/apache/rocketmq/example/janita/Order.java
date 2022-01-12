package org.apache.rocketmq.example.janita;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Order
 *
 * @author zhucj
 * @since 20220120
 */
@Data
@AllArgsConstructor
public class Order {

    private Integer orderId;

    private String name;

    private String money;
}
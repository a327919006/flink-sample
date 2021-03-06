package com.cn.flink.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Chen Nan
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorDataResult {
    private Long id;
    private String name;
    private Double value;
    private Long timestamp;
}

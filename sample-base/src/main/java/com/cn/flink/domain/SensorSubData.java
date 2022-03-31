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
public class SensorSubData {
    private Long id;
    private String name;
    private Double value;
    private Long timestamp;

    @Override
    public String toString() {
        return "SensorSubData{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}

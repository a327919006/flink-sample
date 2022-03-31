package com.cn.flink.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author Chen Nan
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorDataDetail {
    private SensorData sensorData;
    private List<SensorSubData> sensorSubDataList;

    @Override
    public String toString() {
        return "SensorDataDetail{" +
                "sensorData=" + sensorData +
                ", sensorSubDataList=" + sensorSubDataList +
                '}';
    }
}

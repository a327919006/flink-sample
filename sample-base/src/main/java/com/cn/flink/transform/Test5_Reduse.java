package com.cn.flink.transform;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * reduce，更复杂的应用场景，相比min等只支持单个属性计算，reduce支持自定义逻辑
 *
 * @author Chen Nan
 */
public class Test5_Reduse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        env.readTextFile(file.getAbsolutePath())
                .map((MapFunction<String, SensorData>) value -> {
                    String[] split = value.split(",");
                    SensorData sensorData = new SensorData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorData.class))
                // 根据ID分组
                .keyBy((KeySelector<SensorData, Long>) SensorData::getId)
                // 取每个ID中value的最大值和最新时间
                .reduce((ReduceFunction<SensorData>) (oldData, newData) ->
                        new SensorData(newData.getId(), newData.getName(),
                                Math.max(newData.getValue(), oldData.getValue()),
                                newData.getTimestamp()))
                .print();

        env.execute();
    }
}

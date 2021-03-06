package com.cn.flink.transform;

import com.cn.flink.domain.SensorData;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * keyBy分组，支持：
 * sum求和
 * min最小值
 * max最大值
 * minBy最小值-first为true时与min结果相同，为false时，新数据与最小值相同时，取新数据
 * maxBy最大值-同理
 *
 * @author Chen Nan
 */
public class Test4_KeyBy {
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
                // 取每个ID中value的最大值
//                .max("value")
                .maxBy("value", false)
                .print();

        env.execute();
    }
}

package com.cn.flink.windows;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 时间语义，事件时间
 *
 * @author Chen Nan
 */
public class Test5_EventWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 生成Watermark的周期，默认200毫秒
        env.getConfig().setAutoWatermarkInterval(200);


        env.socketTextStream("127.0.0.1", 7777)
                .map((MapFunction<String, SensorData>) value -> {
                    String[] split = value.split(",");
                    SensorData sensorData = new SensorData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorData.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorData>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<SensorData>) (element, recordTimestamp) -> element.getTimestamp())
                )
                .keyBy((KeySelector<SensorData, Long>) SensorData::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .maxBy("value")
                .print();


        env.execute();
    }
}

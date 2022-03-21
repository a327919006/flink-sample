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
 * 时间语义，事件时间，水位线Watermark
 * 测试数据：
 * 1,sensor1,11,1640000001000
 * 1,sensor1,12,1640000002000
 * 1,sensor1,13,1640000003000
 * 1,sensor1,14,1640000004000
 * 1,sensor1,15,1640000005000
 * 1,sensor1,16,1640000006000
 * 1,sensor1,22,1640000012000
 * 1,sensor1,117,1640000007000
 * 1,sensor1,118,1640000008000
 * 1,sensor1,119,1640000009000
 * 1,sensor1,20,1640000010000
 * 1,sensor1,21,1640000011000
 * 1,sensor1,23,1640000013000
 * 1,sensor1,25,1640000015000
 * 1,sensor1,27,1640000017000
 * 1,sensor1,29,1640000019000
 * 1,sensor1,30,1640000020000
 * 1,sensor1,31,1640000021000
 * 1,sensor1,32,1640000022000
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
                        // 水位线生成器，forBoundedOutOfOrderness，针对乱序流，允许x秒的延迟
                        WatermarkStrategy.<SensorData>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        // forMonotonousTimestamps，不允许延迟，即Duration设为0s
                        // WatermarkStrategy.<SensorData>forMonotonousTimestamps()
                                // 指定事件时间的字段
                                .withTimestampAssigner((SerializableTimestampAssigner<SensorData>)
                                        (element, recordTimestamp) -> element.getTimestamp())
                )
                .keyBy((KeySelector<SensorData, Long>) SensorData::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .maxBy("value")
                .print();


        env.execute();
    }
}

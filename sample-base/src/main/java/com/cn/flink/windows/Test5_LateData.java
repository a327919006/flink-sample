package com.cn.flink.windows;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 迟到数据
 * 测试数据：
 * 1,sensor1,11,1640000001000
 * 1,sensor1,12,1640000002000
 * 1,sensor1,13,1640000003000
 * 1,sensor1,14,1640000004000
 * 1,sensor1,15,1640000005000
 * 1,sensor1,16,1640000006000
 * 1,sensor1,22,1640000012000
 * 1,sensor1,23,1640000001000
 * 1,sensor1,18,1640000008000
 * 1,sensor1,24,1640000009000
 * 1,sensor1,25,1640000015000
 * 1,sensor1,26,1640000007000
 *
 * @author Chen Nan
 */
public class Test5_LateData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OutputTag<SensorData> outputTag = new OutputTag<SensorData>("late") {
        };

        SingleOutputStreamOperator<SensorData> dataStream = env.socketTextStream("127.0.0.1", 7777)
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
                                .withTimestampAssigner((SerializableTimestampAssigner<SensorData>)
                                        (element, recordTimestamp) -> element.getTimestamp())
                )
                // 使用window需先keyBy，如根据ID分组
                .keyBy((KeySelector<SensorData, Long>) SensorData::getId)
                // 滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 设置窗口延迟x秒关闭，即示例中窗口为10s，允许乱序2s，此时12s的数据到达时会先输出一次0-10s的结果
                // 如果在15s数据到达前，还有0-10s内的数据，则还会触发计算，并输出结果
                // 如果在15s数据到达后，还有0-10s内的数据，则会写入测输出流
                .allowedLateness(Time.seconds(3))
                // 使用侧输出流，将窗口关闭后才到达的数据打上标签，防止数据丢失
                .sideOutputLateData(outputTag)
                .maxBy("value");

        DataStreamSink<SensorData> sideOutput = dataStream.getSideOutput(outputTag).print("side");

        dataStream.print("data");

        env.execute();
    }
}

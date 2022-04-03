package com.cn.flink.transform;

import com.cn.flink.domain.SensorData;
import com.cn.flink.domain.SensorDataDetail;
import com.cn.flink.domain.SensorSubData;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * IntervalJoin间隔连接，同样可以实现innerJoin效果，也是1对1join
 * 不同的是，IntervalJoin没有windows的概念，由一个流中的事件时间，取某个时间范围内的另一个流的数据进行join
 * 有两个流，a流的数据到达后，b流的数据在a流的时间前x秒到后x秒内到达都能与a连接
 * 示例数据：
 * 左流
 * 1,sensor1,10,1640966401000
 * 1,sensor1,11,1640966402000
 * 右流
 * 1,sensor1,11,1640966400000
 * 1,sensor1,12,1640966403000
 * 1,sensor1,11,1640966406000
 * 1,sensor1,11,1640966407000
 *
 * @author Chen Nan
 */
public class Test13_IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorData> streamLeft = env.socketTextStream("127.0.0.1", 7777)
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
                        WatermarkStrategy.<SensorData>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<SensorData>)
                                        (element, recordTimestamp) -> element.getTimestamp())
                );

        DataStream<SensorSubData> streamRight = env.socketTextStream("127.0.0.1", 8888)
                .map((MapFunction<String, SensorSubData>) value -> {
                    String[] split = value.split(",");
                    SensorSubData sensorData = new SensorSubData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorSubData.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorSubData>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<SensorSubData>)
                                        (element, recordTimestamp) -> element.getTimestamp())
                );

        streamLeft.keyBy(SensorData::getId)
                .intervalJoin(streamRight.keyBy(SensorSubData::getId))
                // 默认为事件时间
                .inEventTime()
                // 左流数据时间正负5秒内的右流数据，都会join一次
                .between(Time.seconds(-5L), Time.seconds(5))
                .process(new ProcessJoinFunction<SensorData, SensorSubData, SensorDataDetail>() {

                    @Override
                    public void processElement(SensorData left, SensorSubData right, Context ctx, Collector<SensorDataDetail> out) throws Exception {
                        SensorDataDetail detail = new SensorDataDetail();
                        detail.setSensorData(left);
                        detail.setSensorSubDataList(Collections.singletonList(right));
                        out.collect(detail);
                    }
                })
                .print();

        env.execute();
    }
}

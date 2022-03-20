package com.cn.flink.tableapi;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.File;
import java.time.Duration;

/**
 * 获取rowtime，事件事件
 *
 * @author Chen Nan
 */
public class Test7_EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        DataStream<SensorData> dataStream = env.readTextFile(file.getAbsolutePath())
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
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(dataStream, "id, name, value, timestamp.rowtime as time");

        table.printSchema();
        tableEnv.toDataStream(table, Row.class).print();
        env.execute();
    }
}

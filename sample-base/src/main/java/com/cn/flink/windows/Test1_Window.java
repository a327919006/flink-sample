package com.cn.flink.windows;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;

/**
 * Window:
 * TumblingWindow
 * SlidingWindow
 * SessionWindow
 * GlobalWindow-count使用
 *
 * @author Chen Nan
 */
public class Test1_Window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
                // 使用window需先keyBy，如根据ID分组
                .keyBy((KeySelector<SensorData, Long>) SensorData::getId)
                // 滚动时间窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 滑动时间窗口
                // .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // 会话窗口
                // .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                // 滚动计数窗口
                // .countWindow(10)
                // 滑动计数窗口
                // .countWindow(10, 2)
                .aggregate(new AggregateFunction<SensorData, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorData value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                })
                .print();

        env.execute();
    }
}

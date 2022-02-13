package com.cn.flink.windows;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 使用全量窗口函数
 *
 * @author Chen Nan
 */
public class Test2_FullWindow {
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
                .apply(new WindowFunction<SensorData, Integer, Long, TimeWindow>() {
                    @Override
                    public void apply(Long key, TimeWindow window, Iterable<SensorData> input, Collector<Integer> out) throws Exception {
                        long start = window.getStart();
                        long end = window.getEnd();
                        AtomicInteger count = new AtomicInteger();
                        input.forEach(data -> count.getAndIncrement());
                        int result = count.get();
                        System.out.println("star=" + start + " end=" + end + " key=" + key + " result=" + result);
                        out.collect(result);
                    }
                })
                .print();

        env.execute();
    }
}

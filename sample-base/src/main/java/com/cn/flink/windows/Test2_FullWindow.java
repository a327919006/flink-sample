package com.cn.flink.windows;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
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
                // WindowFunction和ProcessWindowFunction两种写法都可以
                // .process(new ProcessWindowFunction<SensorData, Tuple4<Long, Long, Long, Integer>, Long, TimeWindow>() {
                //     @Override
                //     public void process(Long key, Context context, Iterable<SensorData> elements, Collector<Tuple4<Long, Long, Long, Integer>> out) throws Exception {
                //
                //     }
                // })
                // 计算时间窗口内每个ID出现的次数
                .apply(new WindowFunction<SensorData, Tuple4<Long, Long, Long, Integer>, Long, TimeWindow>() {
                    @Override
                    public void apply(Long key, TimeWindow window, Iterable<SensorData> input,
                                      Collector<Tuple4<Long, Long, Long, Integer>> out) throws Exception {
                        long start = window.getStart();
                        long end = window.getEnd();
                        AtomicInteger count = new AtomicInteger();
                        input.forEach(data -> count.getAndIncrement());
                        int result = count.get();
                        out.collect(Tuple4.of(start, end, key, result));
                    }
                })
                .print();

        env.execute();
    }
}

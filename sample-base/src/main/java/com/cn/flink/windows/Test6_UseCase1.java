package com.cn.flink.windows;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Chen Nan
 */
public class Test6_UseCase1 {
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
                .keyBy((KeySelector<SensorData, Long>) SensorData::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 自定义触发规则，触发计算窗口内的数据，内置EventTimeTrigger、ProcessingTimeTrigger、CountTrigger
                // .trigger()
                // 自定义移除器，根据自定义业务移除某些数据
                // .evictor()
                // AggregateFunction计算窗口内每个ID出现的次数
                // ProcessWindowFunction拼接上时间窗口信息
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
                }, new ProcessWindowFunction<Integer, String, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<Integer> elements,
                                        Collector<String> out) throws Exception {
                        Integer count = elements.iterator().next();
                        String result = "key=" + key + " start=" + context.window().getStart()
                                + " end=" + context.window().getEnd() + " count=" + count;
                        out.collect(result);
                    }
                })
                .print();

        env.execute();
    }
}

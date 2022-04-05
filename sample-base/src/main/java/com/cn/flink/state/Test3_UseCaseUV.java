package com.cn.flink.state;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 状态使用案例：统计总数据量
 *
 * @author Chen Nan
 */
public class Test3_UseCaseUV {
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
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorData>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<SensorData>)
                                        (element, recordTimestamp) -> element.getTimestamp())
                )
                .keyBy((KeySelector<SensorData, Long>) SensorData::getId)
                .process(new KeyedProcessFunction<Long, SensorData, String>() {

                    // 用于保存数据量
                    private ValueState<Long> countState;
                    // 用于保存定时器触发时间
                    private ValueState<Long> timeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("countState", Long.class));
                        timeState = getRuntimeContext().getState(new ValueStateDescriptor<>("timeState", Long.class));
                    }

                    @Override
                    public void processElement(SensorData value, Context ctx, Collector<String> out) throws Exception {
                        Long count = countState.value();
                        countState.update(count == null ? 1L : count + 1);

                        if (timeState.value() == null) {
                            Long time = value.getTimestamp() + 5000 - 1;
                            System.out.println("time=" + time);
                            ctx.timerService().registerEventTimeTimer(time);
                            timeState.update(time);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("id=" + ctx.getCurrentKey()
                                + " count=" + countState.value()
                                + " time=" + timestamp);

                        // 清空时间状态，直到下个数据到后才会注册新定时任务
                        timeState.clear();

                        // 每次输出后都重新注册定时器，不管期间有没数据，都会触发
                        // ctx.timerService().registerEventTimeTimer(timestamp + 5000);
                        // timeState.update(timestamp + 5000);
                    }

                })
                .print();

        env.execute();
    }
}

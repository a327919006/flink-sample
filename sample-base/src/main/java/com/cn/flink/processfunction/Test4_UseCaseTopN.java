package com.cn.flink.processfunction;

import com.cn.flink.domain.SensorData;
import com.cn.flink.domain.SensorDataCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 使用案例：每隔5秒输出最近5s内数据量最大的前N个sensor
 * 思路
 *
 * @author Chen Nan
 */
public class Test4_UseCaseTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorDataCount> dataStream = env.socketTextStream("127.0.0.1", 7777)
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
                }, new ProcessWindowFunction<Integer, SensorDataCount, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<Integer> elements,
                                        Collector<SensorDataCount> out) throws Exception {
                        long id = key;
                        int count = elements.iterator().next();
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        SensorDataCount sensorDataCount = new SensorDataCount();
                        sensorDataCount.setId(id);
                        sensorDataCount.setCount(count);
                        sensorDataCount.setWindowStart(start);
                        sensorDataCount.setWindowEnd(end);
                        out.collect(sensorDataCount);
                    }
                });

        dataStream.print("data");

        dataStream.keyBy(SensorDataCount::getWindowEnd)
                .process(new KeyedProcessFunction<Long, SensorDataCount, SensorDataCount>() {

                    private ListState<SensorDataCount> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<>("data", SensorDataCount.class));
                    }

                    @Override
                    public void processElement(SensorDataCount value, Context ctx, Collector<SensorDataCount> out) throws Exception {
                        listState.add(value);

                        ctx.timerService().registerProcessingTimeTimer(ctx.getCurrentKey() + 1);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorDataCount> out) throws Exception {
                        List<SensorDataCount> list = new ArrayList<>();
                        for (SensorDataCount data : listState.get()) {
                            list.add(data);
                        }
                        Collections.sort(list);

                        for (int i = 0; i < 2; i++) {
                            if (list.size() > i) {
                                out.collect(list.get(i));
                            }
                        }
                    }
                })
                .print("top2");

        env.execute();
    }
}

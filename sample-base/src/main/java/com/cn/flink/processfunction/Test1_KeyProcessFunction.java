package com.cn.flink.processfunction;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.File;

/**
 * KeyProcessFunction，Timer定时任务
 *
 * @author Chen Nan
 */
public class Test1_KeyProcessFunction {
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
                .process(new KeyedProcessFunction<Long, SensorData, Double>() {
                    @Override
                    public void processElement(SensorData value, Context ctx, Collector<Double> out) throws Exception {
                        out.collect(value.getValue());

                        TimerService timerService = ctx.timerService();
                        long currTime = timerService.currentProcessingTime();
                        timerService.registerProcessingTimeTimer(currTime + 3000);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Double> out) throws Exception {
                        System.out.println(timestamp + "定时器触发");
                        out.collect(123D);
                    }
                })
                .print();

        env.execute();
    }
}

package com.cn.flink.processfunction;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 使用案例：value在10秒内连续上升
 *
 * @author Chen Nan
 */
public class Test2_UseCaseTimer {
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
                .process(new TempFunction())
                .print();

        env.execute();
    }

    public static class TempFunction extends KeyedProcessFunction<Long, SensorData, String> {
        // 10秒内连续上升
        private Integer interval = 10;

        private ValueState<Double> lastValueState;
        private ValueState<Long> timeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("value-state", Double.class));
            timeState = getRuntimeContext().getState(new ValueStateDescriptor<>("time-state", Long.class));
        }

        @Override
        public void processElement(SensorData value, Context ctx, Collector<String> out) throws Exception {
            Double lastValue = lastValueState.value();
            Long time = timeState.value();
            if (lastValue == null) {
                lastValue = Double.MIN_VALUE;
            }

            if (value.getValue() > lastValue && time == null) {
                long ts = ctx.timerService().currentProcessingTime() + interval * 1000;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timeState.update(ts);
            }

            if (value.getValue() <= lastValue && time != null) {
                timeState.clear();
                ctx.timerService().deleteProcessingTimeTimer(time);
            }

            lastValueState.update(value.getValue());
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Double lastValue = lastValueState.value();
            String msg = ctx.getCurrentKey() + "在" + interval + "s内数据连续上升，最新值为" + lastValue + "，异常告警";
            out.collect(msg);
            timeState.clear();
        }

        @Override
        public void close() throws Exception {
            lastValueState.clear();
        }
    }
}

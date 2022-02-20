package com.cn.flink.state;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 键控状态，针对每个Key，有一个状态
 *
 * @author Chen Nan
 */
public class Test2_KeyState {
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
                .map(new MyMapFunction())
                .print();

        env.execute();
    }

    public static class MyMapFunction extends RichMapFunction<SensorData, Integer> {
        private ValueState<Integer> keyState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyState = getRuntimeContext().getState(new ValueStateDescriptor<>("key-count", Integer.class));
        }

        @Override
        public Integer map(SensorData value) throws Exception {
            Integer count = keyState.value();
            if (count == null) {
                count = 0;
            }
            ++count;
            keyState.update(count);
            return count;
        }
    }
}

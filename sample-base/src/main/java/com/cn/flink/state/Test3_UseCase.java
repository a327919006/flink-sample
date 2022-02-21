package com.cn.flink.state;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 状态使用案例：如当前后两个数据的value差为10时，进行告警
 *
 * @author Chen Nan
 */
public class Test3_UseCase {
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
                .flatMap(new ValueDiffFunction())
                .print();

        env.execute();
    }

    public static class ValueDiffFunction extends RichFlatMapFunction<SensorData, Tuple3<Long, Double, Double>> {
        private ValueState<Double> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("value-count", Double.class));
        }

        @Override
        public void flatMap(SensorData value, Collector<Tuple3<Long, Double, Double>> out) throws Exception {
            Double lastValue = valueState.value();
            if (lastValue != null) {
                double diff = Math.abs(lastValue - value.getValue());
                if (diff > 10) {
                    out.collect(Tuple3.of(value.getId(), lastValue, value.getValue()));
                }
            }
            valueState.update(value.getValue());
        }

        @Override
        public void close() throws Exception {
            valueState.clear();
        }
    }
}

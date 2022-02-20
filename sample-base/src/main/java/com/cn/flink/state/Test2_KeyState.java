package com.cn.flink.state;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;

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
        private ValueState<Integer> valueState;
        private ListState<String> listState;
        private MapState<String, Integer> mapState;
        private ReducingState<String> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("value-count", Integer.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<>("list-state", String.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("map-state", String.class, Integer.class));
            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("reducing-state", (ReduceFunction<String>) (value1, value2) -> value1 + value2, String.class));
        }

        @Override
        public Integer map(SensorData value) throws Exception {
            Integer count = valueState.value();
            if (count == null) {
                count = 0;
            }
            ++count;
            valueState.update(count);

            listState.add("test" + count);
            String listStateValue;
            Iterator<String> iterator = listState.get().iterator();
            while (iterator.hasNext()) {
                listStateValue = iterator.next();
                System.out.println(listStateValue);
            }

            mapState.put("test" + count, count);

            return count;
        }
    }
}

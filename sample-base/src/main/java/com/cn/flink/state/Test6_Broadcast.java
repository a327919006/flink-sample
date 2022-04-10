package com.cn.flink.state;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 广播流，实现动态配置
 * 普通的数据流 连接 广播流，广播流负责收到配置更新后通过状态将配置传递到数据流
 * 数据流使用最小的配置处理数据
 * 举例：配置项为数据值的最小值，数据流处理数据时判断数据值是否大于最小值，大于才输出
 *
 * @author Chen Nan
 */
public class Test6_Broadcast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorData> dataStream = env.socketTextStream("127.0.0.1", 7777)
                .map((MapFunction<String, SensorData>) value -> {
                    String[] split = value.split(",");
                    SensorData sensorData = new SensorData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorData.class));

        DataStream<Double> ruleStream = env.socketTextStream("127.0.0.1", 8888)
                .map((MapFunction<String, Double>) Double::parseDouble, TypeInformation.of(Double.class));


        MapStateDescriptor<Void, Double> descriptor = new MapStateDescriptor<>("rule", Void.class, Double.class);
        BroadcastStream<Double> broadcast = ruleStream.broadcast(descriptor);

        dataStream.keyBy(SensorData::getId)
                .connect(broadcast)
                .process(new KeyedBroadcastProcessFunction<Long, SensorData, Double, SensorData>() {
                    private ValueState<Double> minValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        minValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("minValue", Double.class));
                    }

                    @Override
                    public void processElement(SensorData value, ReadOnlyContext ctx, Collector<SensorData> out) throws Exception {
                        Double minValue = minValueState.value();
                        if (minValue == null) {
                            minValue = 0D;
                        }
                        if (value.getValue().compareTo(minValue) > 0) {
                            out.collect(value);
                        }
                    }

                    @Override
                    public void processBroadcastElement(Double value, Context ctx, Collector<SensorData> out) throws Exception {
                        minValueState.update(value);
                    }
                })
                .print();

        env.execute();
    }
}

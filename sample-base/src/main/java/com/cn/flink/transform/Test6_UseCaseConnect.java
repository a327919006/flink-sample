package com.cn.flink.transform;

import com.cn.flink.domain.SensorData;
import com.cn.flink.domain.SensorDataDetail;
import com.cn.flink.domain.SensorSubData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Chen Nan
 */
public class Test6_UseCaseConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorData> streamLeft = env.socketTextStream("127.0.0.1", 7777)
                .map((MapFunction<String, SensorData>) value -> {
                    String[] split = value.split(",");
                    SensorData sensorData = new SensorData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorData.class));

        DataStream<SensorSubData> streamRight = env.socketTextStream("127.0.0.1", 8888)
                .map((MapFunction<String, SensorSubData>) value -> {
                    String[] split = value.split(",");
                    SensorSubData sensorData = new SensorSubData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorSubData.class));

        streamLeft.keyBy(SensorData::getId)
                .connect(streamRight.keyBy(SensorSubData::getId))
                .process(new KeyedCoProcessFunction<Long, SensorData, SensorSubData, SensorDataDetail>() {

                    private ValueState<SensorData> dataState;
                    private ListState<SensorSubData> subDataListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        dataState = getRuntimeContext().getState(new ValueStateDescriptor<>("data", SensorData.class));
                        subDataListState = getRuntimeContext().getListState(new ListStateDescriptor<>("subData", SensorSubData.class));
                    }

                    @Override
                    public void processElement1(SensorData value, Context ctx, Collector<SensorDataDetail> out) throws Exception {
                        dataState.update(value);
                        long time = ctx.timerService().currentProcessingTime() + 5000;
                        ctx.timerService().registerProcessingTimeTimer(time);
                    }

                    @Override
                    public void processElement2(SensorSubData value, Context ctx, Collector<SensorDataDetail> out) throws Exception {
                        subDataListState.add(value);
                        long time = ctx.timerService().currentProcessingTime() + 5000;
                        ctx.timerService().registerProcessingTimeTimer(time);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorDataDetail> out) throws Exception {
                        SensorData sensorData = dataState.value();
                        if (sensorData == null) {
                            System.out.println("clear");
                            subDataListState.clear();
                            return;
                        }
                        List<SensorSubData> subDataList = new ArrayList<>();
                        for (SensorSubData data : subDataListState.get()) {
                            subDataList.add(data);
                        }

                        SensorDataDetail detail = new SensorDataDetail();
                        detail.setSensorData(sensorData);
                        detail.setSensorSubDataList(subDataList);
                        out.collect(detail);

                        dataState.clear();
                        subDataListState.clear();
                    }
                })
                .print();
        env.execute();
    }
}

package com.cn.flink.state;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * 使用算子状态，实现自定义sink，每10个数据输出一次
 *
 * @author Chen Nan
 */
public class Test5_UseCaseSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        env.readTextFile(file.getAbsolutePath())
                .map((MapFunction<String, SensorData>) value -> {
                    String[] split = value.split(",");
                    SensorData sensorData = new SensorData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorData.class))
                .addSink(new MySinkFunction());

        env.execute();
    }

    public static class MySinkFunction implements CheckpointedFunction, SinkFunction<SensorData> {

        private int maxCount = 10;
        private List<SensorData> list = new ArrayList<>();
        private ListState<SensorData> listState;

        @Override
        public void invoke(SensorData value, Context context) throws Exception {
            list.add(value);

            if (list.size() >= maxCount) {
                System.out.println("===========开始输出===========");
                for (SensorData sensorData : list) {
                    System.out.println(sensorData);
                }
                System.out.println("===========完成输出===========");
                list.clear();
            }
        }

        /**
         * 创建快照时触发调用，将数据保存到状态中
         * 比如kafka，需保存offset
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();

            // 将本地变量中数据保存到状态
            for (SensorData sensorData : list) {
                listState.add(sensorData);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<SensorData> descriptor = new ListStateDescriptor<>("sensorDataList", SensorData.class);
            listState = context.getOperatorStateStore().getListState(descriptor);

            // 如果是从故障中恢复状态，则把数据从状态中写入本地变量
            if (context.isRestored()) {
                for (SensorData sensorData : listState.get()) {
                    list.add(sensorData);
                }
            }
        }
    }
}

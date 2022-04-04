package com.cn.flink.state;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 算子状态，针对当前算子的状态，作用范围仅在当前算子的并行子任务内有效
 * 当前算子设置为多并行度时，每个子任务不共享算子状态，都是在独立的内存空间。
 *
 * @author Chen Nan
 */
public class Test1_OperatorState {
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
                .map(new MyMapFunction())
                .print();

        env.execute();
    }

    public static class MyMapFunction implements MapFunction<SensorData, Integer>, CheckpointedFunction {

        private Integer count = 0;
        private transient ListState<Integer> checkpointedState;


        @Override
        public Integer map(SensorData value) throws Exception {
            return ++count;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            checkpointedState.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Integer> descriptor =
                    new ListStateDescriptor<>(
                            "buffered-elements",
                            TypeInformation.of(new TypeHint<Integer>() {
                            }));

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                count = checkpointedState.get().iterator().next();
            }
        }
    }
}

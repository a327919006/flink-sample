package com.cn.flink.state;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 检查点和重启策略
 *
 * @author Chen Nan
 */
public class Test7_CheckPoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置CheckPoint
        env.enableCheckpointing(1000);
        // checkPoint执行间隔，同上enableCheckpointing(1000)
        env.getCheckpointConfig().setCheckpointInterval(1000);
        // 模式：精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 超时时间，防止checkPoint存储端异常导致超时
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同时执行checkPoint的数量，默认1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 间歇时间，前一次checkPoint完成到下一次checkPoint开始之前的时间间隔，默认0
        // 假设每1000毫秒cp一次，上一次cp执行了950毫秒，如果间歇为0，则下次cp在上次cp结束50毫秒后就开始
        // 如果间歇为100，则上次cp结束50毫秒后不会开始下次cp，需要间歇100毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(0);
        // 容忍cp失败次数，默认-1无限次，假设为0，则不允许cp失败，cp失败则当做任务失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);


        // 设置故障重启策略
        // 固定延时重启，每隔10秒重启一次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        // 失败率，10分钟内重启3次，每次间隔5秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.seconds(5)));


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
                .max("value")
                .print();

        env.execute();
    }
}

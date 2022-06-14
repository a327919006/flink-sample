package com.cn.flink.cdc.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvironmentUtils {

    public static StreamExecutionEnvironment getEnv(Class clazz) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Boolean isHdfs = true;

//        String hdfsPath = "hdfs://192.168.5.141:56534";
//        String checkpointDir = hdfsPath + "/test/flink/" + clazz.getSimpleName();
        String hdfsPath = "alluxio://192.168.5.143:19998";
        String checkpointDir = hdfsPath + "/mnt/hdfs/flink/" + clazz.getSimpleName();
        // 获取当前的配置的SAVEPOINT_PATH 是否为空 如果为空则去checkpoint去获取
        if (isHdfs) {
            Configuration config = new Configuration();
            String checkpointUrl = HdfsUtils.getCheckpointUrl(hdfsPath, checkpointDir);
            //获取到的checkpoint是否为空 不为空设置
            if (StringUtils.isNotEmpty(checkpointUrl)) {
                config.setString(SavepointConfigOptions.SAVEPOINT_PATH, checkpointUrl);
                config.set(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, true);
                env = StreamExecutionEnvironment.getExecutionEnvironment(config);
            }
        }

        // 每x毫秒生成一次检查点
        env.enableCheckpointing(10000);
        // 一致性策略
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在x毫秒内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(100000);
        // 同一时间允许的检查点个数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的 Checkpoint (重启策略)
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 重启策略
        env.setRestartStrategy(RestartStrategies.fallBackRestart());
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);

        return env;
    }

}

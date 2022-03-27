package com.cn.flink.transform;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * 重分区，不常用
 *
 * @author Chen Nan
 */
public class Test9_Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File file = new File("sample-base\\src\\main\\resources\\data.txt");
        DataStream<SensorData> dataStream = env.readTextFile(file.getAbsolutePath())
                .map((MapFunction<String, SensorData>) value -> {
                    String[] split = value.split(",");
                    SensorData sensorData = new SensorData();
                    sensorData.setId(Long.parseLong(split[0]));
                    sensorData.setName(split[1]);
                    sensorData.setValue(Double.parseDouble(split[2]));
                    sensorData.setTimestamp(Long.parseLong(split[3]));
                    return sensorData;
                }, TypeInformation.of(SensorData.class));

        // 默认分区
        // dataStream.print("input").setParallelism(4);

        // 随机分区
        // dataStream.shuffle().print("shuffle").setParallelism(4);

        // 轮询分区
        // dataStream.rebalance().print("rebalance").setParallelism(4);

        // 重缩放分区，假设上游并行度2下游为4，则上游的数据会被分成两个小组，分别发往下游的两个分区，即上游其中一个小组只有在下游某两个小组中出现
        // dataStream.rescale().print("rescale").setParallelism(4);

        // 广播，每一条数据会被所有分区收到
        // dataStream.broadcast().print("broadcast").setParallelism(4);

        // 全局分区，所有数据都分配到一个分区
        // dataStream.global().print("global").setParallelism(4);

        // 自定义分区规则，先定义key，然后按key的值分配到不同分区
        dataStream.partitionCustom(
                new Partitioner<Long>() {
                    @Override
                    public int partition(Long key, int numPartitions) {
                        return key.intValue() % numPartitions;
                    }
                }, new KeySelector<SensorData, Long>() {
                    @Override
                    public Long getKey(SensorData value) throws Exception {
                        return value.getId();
                    }
                })
                .print("partitionCustom")
                .setParallelism(4);

        env.execute();
    }
}

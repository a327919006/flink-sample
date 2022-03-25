package com.cn.flink.source;

import com.cn.flink.domain.SensorData;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * 从集合读取数据
 * <p>
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/datastream/overview/
 *
 * @author Chen Nan
 */
public class SourceTest1_Collection {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设为1，防止乱序
        env.setParallelism(1);

        List<SensorData> dataList = Arrays.asList(
                new SensorData(1L, "sensor1", 10D, System.currentTimeMillis()),
                new SensorData(2L, "sensor2", 20D, System.currentTimeMillis()),
                new SensorData(3L, "sensor3", 30D, System.currentTimeMillis())
        );
        // 从集合获取fromCollection
        env.fromCollection(dataList).print("element");

        // 从元素获取fromElements
        env.fromElements(1, 2, 3).print("collection");

        env.execute("CollectionSourceTest");
    }
}

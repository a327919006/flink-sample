package com.cn.flink.sink;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.io.File;

/**
 * sink-将数据写入Redis
 *
 * @author Chen Nan
 */
public class SinkTest2_Redis {

    public static void main(String[] args) throws Exception {
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                .setTimeout(10000)
                .setDatabase(0)
                .build();
        RedisSink<SensorData> sink = new RedisSink<>(config, new RedisMapper<SensorData>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "sensor_data");
            }

            @Override
            public String getKeyFromData(SensorData data) {
                return String.valueOf(data.getId());
            }

            @Override
            public String getValueFromData(SensorData data) {
                return String.valueOf(data.getValue());
            }
        });


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
                .addSink(sink);

        env.execute();
    }
}

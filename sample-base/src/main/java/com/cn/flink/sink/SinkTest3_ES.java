package com.cn.flink.sink;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * sink-将数据写入ElasticSearch7
 *
 * @author Chen Nan
 */
public class SinkTest3_ES {

    public static void main(String[] args) throws Exception {
        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("127.0.0.1", 9200));
        ElasticsearchSink<SensorData> sink = new ElasticsearchSink
                .Builder<>(hosts,
                (ElasticsearchSinkFunction<SensorData>) (sensorData, runtimeContext, requestIndexer) -> {
                    Map<String, String> dataSource = new HashMap<>();
                    dataSource.put("id", sensorData.getId().toString());
                    dataSource.put("name", sensorData.getName());
                    dataSource.put("value", sensorData.getValue().toString());
                    dataSource.put("time", sensorData.getTimestamp().toString());

                    IndexRequest indexRequest = Requests.indexRequest()
                            .index("sensor_data")
                            .source(dataSource);
                    requestIndexer.add(indexRequest);
                })
                .build();

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

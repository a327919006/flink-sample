package com.cn.flink.sink;

import com.cn.flink.domain.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.File;
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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 如果是es集群，则添加多个节点
        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("127.0.0.1", 9200));

        ElasticsearchSink.Builder<SensorData> sinkBuilder = new ElasticsearchSink
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
                });

        // 如果ES有密码，则需设置
        String username = "username";
        String password = "password";
        RestClientFactory restClientFactory = (RestClientFactory) restClientBuilder -> {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));
            restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                httpAsyncClientBuilder.disableAuthCaching();
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            });
        };
        sinkBuilder.setRestClientFactory(restClientFactory);
        // 批量写入条数
        sinkBuilder.setBulkFlushMaxActions(1000);
        // 批量写入间隔
        sinkBuilder.setBulkFlushInterval(500);

        ElasticsearchSink<SensorData> sink = sinkBuilder.build();

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

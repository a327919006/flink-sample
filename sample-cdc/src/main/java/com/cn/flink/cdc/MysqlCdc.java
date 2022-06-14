package com.cn.flink.cdc;

import com.cn.flink.cdc.util.EnvironmentUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hdfs.client.HdfsUtils;

/**
 * 入门示例
 * 初始化读：SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={transaction_id=null, ts_sec=1643358413, file=, pos=0}} ConnectRecord{topic='mysql_binlog_source.cntest.user', kafkaPartition=null, key=Struct{id=1}, keySchema=Schema{mysql_binlog_source.cntest.user.Key:STRUCT}, value=Struct{after=Struct{id=1,name=zhangsan,sex=1},source=Struct{version=1.5.4.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1643358413351,db=cntest,table=user,server_id=0,file=,pos=0,row=0},op=r,ts_ms=1643358413354}, valueSchema=Schema{mysql_binlog_source.cntest.user.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
 * 插入：SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={transaction_id=null, ts_sec=1643358432, file=mysql-bin.000026, pos=102301719, gtids=04359ead-4079-11ec-aa5a-0ecc9cb64055:1-1480920,40faa22d-4079-11ec-bb38-7205d54c42a8:1-5537728, row=1, server_id=100, event=2}} ConnectRecord{topic='mysql_binlog_source.cntest.user', kafkaPartition=null, key=Struct{id=2}, keySchema=Schema{mysql_binlog_source.cntest.user.Key:STRUCT}, value=Struct{after=Struct{id=2,name=lisi,sex=0},source=Struct{version=1.5.4.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1643358432000,db=cntest,table=user,server_id=100,gtid=04359ead-4079-11ec-aa5a-0ecc9cb64055:1480921,file=mysql-bin.000026,pos=102301846,row=0},op=c,ts_ms=1643358432003}, valueSchema=Schema{mysql_binlog_source.cntest.user.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
 * 更新：SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={transaction_id=null, ts_sec=1643358438, file=mysql-bin.000026, pos=102304386, gtids=04359ead-4079-11ec-aa5a-0ecc9cb64055:1-1480927,40faa22d-4079-11ec-bb38-7205d54c42a8:1-5537728, row=1, server_id=100, event=2}} ConnectRecord{topic='mysql_binlog_source.cntest.user', kafkaPartition=null, key=Struct{id=2}, keySchema=Schema{mysql_binlog_source.cntest.user.Key:STRUCT}, value=Struct{before=Struct{id=2,name=lisi,sex=0},after=Struct{id=2,name=lisi123,sex=0},source=Struct{version=1.5.4.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1643358438000,db=cntest,table=user,server_id=100,gtid=04359ead-4079-11ec-aa5a-0ecc9cb64055:1480928,file=mysql-bin.000026,pos=102304513,row=0},op=u,ts_ms=1643358438092}, valueSchema=Schema{mysql_binlog_source.cntest.user.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
 * 删除：SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={transaction_id=null, ts_sec=1643358441, file=mysql-bin.000026, pos=102305875, gtids=04359ead-4079-11ec-aa5a-0ecc9cb64055:1-1480931,40faa22d-4079-11ec-bb38-7205d54c42a8:1-5537728, row=1, server_id=100, event=2}} ConnectRecord{topic='mysql_binlog_source.cntest.user', kafkaPartition=null, key=Struct{id=2}, keySchema=Schema{mysql_binlog_source.cntest.user.Key:STRUCT}, value=Struct{before=Struct{id=2,name=lisi123,sex=0},source=Struct{version=1.5.4.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1643358441000,db=cntest,table=user,server_id=100,gtid=04359ead-4079-11ec-aa5a-0ecc9cb64055:1480932,file=mysql-bin.000026,pos=102306002,row=0},op=d,ts_ms=1643358441219}, valueSchema=Schema{mysql_binlog_source.cntest.user.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
 * 关注点：op为r、c、u、d以及before操作前、after操作后
 *
 * @author Chen Nan
 */
public class MysqlCdc {

    public static void main(String[] args) throws Exception {
        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname("192.168.5.131")
                .port(7766)
                .username("cnte")
                .password("Cnte@19808")
                // 允许监听多个数据库
                .databaseList("cntest")
                // 可选，默认为所有表，注意：表名要使用db.table的方式，因为允许监听多个db
                .tableList("cntest.user")
                // 启动选项：
                // initial：会以查询的方式获取当前表内所有数据，然后从binlog的最新位置开始读取
                // earliest：从binlog的最早位置开始读取，注意需要在建库前就配置开启binlog
                // latest：从binlog的最新位置开始读取
                // specificOffset：从binlog指定位置开始读取
                // timestamp：从指定时间之后的binlog开始读取
//                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        // 配置checkpoint，实现重启任务，继续上次binlog位置读取
//        StreamExecutionEnvironment env = EnvironmentUtils.getEnv(MysqlCdc.class);
        env.setParallelism(1);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql-cdc-source")
                .print("print-data")
                .name("test_cdc");

        env.execute();
    }
}

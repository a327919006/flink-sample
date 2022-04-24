package com.cn.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.File;

/**
 * 函数：
 * 标量函数（如将字符串转成大写、数学计算函数、时间戳处理函数等）
 * 聚合函数（Sum求和、Count等）
 * 系统自带函数：https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/functions/systemfunctions/
 * 自定义函数：https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/functions/udfs/
 * 下面为自定义函数：标量函数，一条输入一条输出
 *
 * @author Chen Nan
 */
public class Test12_Function {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        File inputFile = new File("sample-base\\src\\main\\resources\\data.txt");
        String inputFilePath = inputFile.getAbsolutePath();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String inputTableSql = "CREATE TABLE inputTable (" +
                "  id BIGINT," +
                "  name STRING," +
                "  `value` DECIMAL(10,2)," +
                "  `time` BIGINT" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = '" + inputFilePath + "'," +
                "  'format' = 'csv'" +
                ")";
        tableEnv.executeSql(inputTableSql);

        // 注册全局函数MyDataFunction，并指定实现类
        tableEnv.createTemporarySystemFunction("MyDataFunction", MyDataFunction.class);

        // 使用自定义函数进行查询
        Table result = tableEnv.sqlQuery("SELECT id, `value`, " +
                "`time`, MyDataFunction(`time`) " +
                "FROM inputTable");

        tableEnv.toDataStream(result)
                .print();
        env.execute();
    }

    public static class MyDataFunction extends ScalarFunction {
        /**
         * 自定义函数实现判断数据是否是新数据
         */
        public String eval(Long time) {
            long now = System.currentTimeMillis();
            return now - time > 10 * 1000 ? "oldData" : "newData";
        }
    }
}

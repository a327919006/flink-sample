package com.cn.alink;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;

/**
 * Example for KMeans.
 */
public class MyExample {
    public static void main(String[] args) throws Exception {
        String filePath = "E:/data/alink/iris/iris.data";
        String schema = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
        StreamOperator<?> csvSource = new CsvSourceStreamOp()
                .setFilePath(filePath)
                .setSchemaStr(schema)
                .setFieldDelimiter(",");
        csvSource.print();

        CsvSinkStreamOp csvSink = new CsvSinkStreamOp()
                .setFilePath("E:/data/alink/temp/csv_test.csv");

        csvSource.link(csvSink);
        StreamOperator.execute();
    }
}
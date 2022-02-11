package com.kunghsu.apache.flink.tableapi;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 流和表之间的转换
 * author:xuyaokun_kzx
 * date:2022/2/9
 * desc:
*/
public class DataStreamToTableDemo2 {

    public static void main(String[] args) throws Exception {

        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a DataStream
        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 12),
                Row.of("Bob", 10),
                Row.of("Kunghsu", 100));

        //流转成表
        // interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");

// register the Table object as a view and query it
// the query contains an aggregation that produces updates
        tableEnv.createTemporaryView("InputTable", inputTable);
        Table resultTable = tableEnv.sqlQuery(
                "SELECT name, score FROM InputTable where name='Kunghsu'");

        //表转成流
        // interpret the updating Table as a changelog DataStream
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(resultTable, Row.class);
//        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
// add a printing sink and execute in DataStream API
        resultStream.print();
        env.execute();


    }


}

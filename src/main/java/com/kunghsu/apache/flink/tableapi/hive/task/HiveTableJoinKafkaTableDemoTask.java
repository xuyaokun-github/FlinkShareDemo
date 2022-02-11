package com.kunghsu.apache.flink.tableapi.hive.task;

import com.kunghsu.apache.flink.flinkkafka.TopicConstants;
import com.kunghsu.apache.flink.flinkkafka.config.FlinkKafkaConfig;
import com.kunghsu.apache.flink.tableapi.hive.kafkamsg.KafkaAndHiveDemoMsg;
import com.kunghsu.common.utils.JacksonUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.api.Expressions.$;

/**
 * hive表和kafka表连接例子
 * author:xuyaokun_kzx
 * date:2022/2/10
 * desc:
*/
public class HiveTableJoinKafkaTableDemoTask {

    private final static Logger LOGGER = LoggerFactory.getLogger(HiveTableJoinKafkaTableDemoTask.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamTableEnvironment tableEnv = TableEnvironment.create(environmentSettings);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = FlinkKafkaConfig.getFlinkKafkaConsumer(TopicConstants.TOPIC_FLINK_DEMO_1);
        //添加输入源
        DataStream<String> stream = env.addSource(flinkKafkaConsumer);
        SingleOutputStreamOperator<KafkaAndHiveDemoMsg> stream2 = stream.map(new MapFunction<String, KafkaAndHiveDemoMsg>() {
            @Override
            public KafkaAndHiveDemoMsg map(String value) throws Exception {

                LOGGER.info("输入端入参：{}", value);
                System.out.println("输入端入参：" + value);
                KafkaAndHiveDemoMsg flinkTopicMsg = new KafkaAndHiveDemoMsg();
                flinkTopicMsg = JacksonUtils.toJavaObject(value, KafkaAndHiveDemoMsg.class);
                return flinkTopicMsg;
            }
        });

        //将流转成表
        Table inputTable = tableEnv.fromDataStream(stream2, $("createTime"), $("msgId"),
                $("statusCode"), $("tradeId"), $("updateTime"));
//        .as("msgId", "tradeId", "statusCode");

        //获取hive的表
        //hive相关属性
        //定义一个唯一的名称，这个值是可以随意定义的
        String catalogName = "myhive";
        //hive-site.xml的正确位置
        String hiveConfDir = "D:\\hive\\apache-hive-2.3.6-bin\\conf";
        String version = "2.3.6";
        String database = "test";
        HiveCatalog hive = new HiveCatalog(catalogName, "default", hiveConfDir, version);

        tableEnv.registerCatalog(catalogName, hive);
        tableEnv.useCatalog(catalogName);
        tableEnv.useDatabase(database);
//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");

        String sql = "SELECT id, name, destination, partstart FROM people_partition4"
//                + "where name = 'name1'" //带上分区信息
//                (加where会报错的原因是忘记在where前加空格,Caused by: org.apache.flink.sql.parser.impl.ParseException: Encountered "=")
                ;
        String sql2 = "SELECT a.id, a.name, a.destination, a.partstart FROM people_partition4 a"
                + " WHERE a.partstart='20220208'" //带上分区信息
                ;
        String sql3 = "SELECT id, name, destination, partstart FROM people_partition4 "
                + " WHERE partstart='20220208'" //带上分区信息(注意，这里的分区值必须用引号括起来)
                ;
        Table hiveTable = tableEnv.sqlQuery(sql2);

        //两个表连接join
        Table resTable = inputTable.join(hiveTable);

        //求总和
//        resTable = resTable.select($("count(name)").as("cnt"));
//        resTable = resTable.select("count(1)");people_partition4
//        resTable = resTable.select($("name").count().as("cnt"));
//        resTable = resTable.select("name.count as cnt");
        //查询具体字段(查具体每一行)
//        resTable = resTable.select($("name"), $("destination"));
//        resTable = resTable.where("name = 'name1'" + "");
//        resTable = resTable.limit(1000);
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(resTable, Row.class);
        retractStream.print();

        //针对连接后的表进行查询
        //查总数
        Table countResultTable = tableEnv.sqlQuery(
                "SELECT COUNT(1) FROM " + resTable
                        +
                        " where name = 'name1'"
        );
        DataStream<Tuple2<Boolean, Row>> retractStream2 = tableEnv.toRetractStream(countResultTable, Row.class);
        retractStream2.print();

        //查所有行
        Table itemResultTable = tableEnv.sqlQuery(
                "SELECT name, destination FROM " + resTable +
                        " where name = 'name1' "
                        + " and " +
                        " partstart='20220208'"
        );
        DataStream<Tuple2<Boolean, Row>> retractStream3 = tableEnv.toRetractStream(itemResultTable, Row.class);
        retractStream3.print();

        System.out.println("开始执行HiveTableJoinKafkaTableDemoTask");
        String plan = env.getExecutionPlan();
        System.out.println("plan:" + plan);
        env.execute();
    }

}

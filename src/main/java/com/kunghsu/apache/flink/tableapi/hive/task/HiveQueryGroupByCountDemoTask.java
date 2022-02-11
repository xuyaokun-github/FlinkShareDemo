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

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 *
 * group by 之后加上count之后，结果会变成多条记录
 * 原因：
 * 跟表是否分区没关
 *
 * author:xuyaokun_kzx
 * date:2022/2/11
 * desc:
*/
public class HiveQueryGroupByCountDemoTask {

    private final static Logger LOGGER = LoggerFactory.getLogger(HiveQueryGroupByCountDemoTask.class);

    /**
     *
     *

     建表语句：
     groupby_count_test
     CREATE TABLE `test.groupby_count_test`(
     `a` string,
     `b` string)
     COMMENT 'This is a groupby_count_test table'
     ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
     LINES  TERMINATED BY '\n';

     * @param args
     */
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        
        FlinkKafkaConsumer<String> flinkKafkaConsumer = FlinkKafkaConfig.getFlinkKafkaConsumer(TopicConstants.TOPIC_FLINK_DEMO_1);
        //添加输入源
        DataStream<String> stream = env.addSource(flinkKafkaConsumer);
        SingleOutputStreamOperator<KafkaAndHiveDemoMsg> stream2 = stream.map(new MapFunction<String, KafkaAndHiveDemoMsg>() {
            @Override
            public KafkaAndHiveDemoMsg map(String value) throws Exception {

                LOGGER.info("输入端入参：{}", value);
                System.out.println("输入端入参：" + value);
                KafkaAndHiveDemoMsg flinkTopicMsg = JacksonUtils.toJavaObject(value, KafkaAndHiveDemoMsg.class);
                return flinkTopicMsg;
            }
        });

        //通过流得到kafka table
        //将流转成表
        Table inputTable = tableEnv.fromDataStream(stream2, $("msgId"), $("statusCode"));

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

        //模拟表名：user_location_partition
        //在这里需要将要用到的列筛选出来
        String queryHiveSql = "SELECT a,b "
                + " FROM groupby_count_test "
//                + " WHERE partstart='20220210'" //带上分区信息(注意，这里的分区值必须用引号括起来)
                ;
        Table hiveTable = tableEnv.sqlQuery(queryHiveSql);
        Table joinResTable = inputTable.join(hiveTable);

        Table countResultTable = tableEnv.sqlQuery(
                    "SELECT  msgId, statusCode, count(msgId) as userCount from (" +
                            "SELECT msgId, statusCode,a,b " +
                            "FROM " + joinResTable +
                            " where msgId=a\n" +
                            ") t group by msgId, statusCode"
        );

//        Table countResultTable = tableEnv.sqlQuery(
//                "SELECT  msgId, statusCode from (" +
//                        "SELECT msgId, statusCode, a, b " +
//                        "FROM " + joinResTable +
//                        " where msgId=a\n" +
//                        ") t group by msgId, statusCode"
//        );

//        Table countResultTable = tableEnv.sqlQuery(
//                        "SELECT msgId, statusCode, a, b " +
//                        "FROM " + joinResTable +
//                        " where msgId=a\n"
//        );

//        countResultTable.execute()

        DataStream<Tuple2<Boolean, Row>> countResultStream = tableEnv.toRetractStream(countResultTable, Row.class);
//        countResultStream.print("countResultStream");

//        //运用窗口，得到一个最大值
//        SingleOutputStreamOperator<ResultWrapVO> resultStream = countResultStream
//                .keyBy(new KeySelector<Tuple2<Boolean, Row>, String>() {
//                    @Override
//                    public String getKey(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
//
//                        String key = booleanRowTuple2.f1.getField(0) + "_" + booleanRowTuple2.f1.getField(1);
//                        return key;
//                    }
//                })
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
//                .apply(new WindowFunction<Tuple2<Boolean, Row>, ResultWrapVO, String, TimeWindow>() {
//                    @Override
//                    public void apply(String s, TimeWindow window, Iterable<Tuple2<Boolean, Row>> input, Collector<ResultWrapVO> out) throws Exception {
//
//                        long max = 0;
//                        List<ResultItem> resultItemList = new ArrayList<>();
//                        Iterator iterator = input.iterator();
//                        while (iterator.hasNext()){
//                            Tuple2<Boolean, Row> tuple2 = (Tuple2<Boolean, Row>) iterator.next();
//                            ResultItem resultItem = new ResultItem();
//                            resultItem.setMsgId((String) tuple2.f1.getField(0));
//                            resultItem.setStatusCode((String) tuple2.f1.getField(1));
//                            resultItemList.add(resultItem);
//                        }
//                        ResultWrapVO resultWrapVO = new ResultWrapVO();
//                        resultWrapVO.setItemList(resultItemList);
//                        out.collect(resultWrapVO);
//                    }
//                });
//
//        resultStream.print("resultStream");
//
//        SingleOutputStreamOperator<String> finalStream = resultStream.process(new ProcessFunction<ResultWrapVO, String>() {
//            @Override
//            public void processElement(ResultWrapVO value, Context ctx, Collector<String> out) throws Exception {
//
//                out.collect("01");
//                value.getItemList().forEach(item->{
//                    out.collect("02");
//                });
//                out.collect("03");
//            }
//        });
//
//        finalStream.print("resultStream");

        System.out.println("开始执行HiveQueryGroupByCountDemoTask");
        env.execute();

    }
    static class ResultItem {

        private String msgId;
        private String statusCode;

        public String getMsgId() {
            return msgId;
        }

        public void setMsgId(String msgId) {
            this.msgId = msgId;
        }

        public String getStatusCode() {
            return statusCode;
        }

        public void setStatusCode(String statusCode) {
            this.statusCode = statusCode;
        }
    }

    static class ResultWrapVO {

        private List<ResultItem> itemList;

        public List<ResultItem> getItemList() {
            return itemList;
        }

        public void setItemList(List<ResultItem> itemList) {
            this.itemList = itemList;
        }
    }

}

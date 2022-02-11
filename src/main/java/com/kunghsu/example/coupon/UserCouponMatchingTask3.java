package com.kunghsu.example.coupon;

import com.kunghsu.apache.flink.flinkkafka.config.FlinkKafkaConfig;
import com.kunghsu.common.utils.DateUtils;
import com.kunghsu.common.utils.JacksonUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import static com.kunghsu.common.utils.DateUtils.PATTERN_YYYY_MM_DD_HH_MM_SS;
import static org.apache.flink.table.api.Expressions.$;

/**
 * 实际案例--根据商户经纬度给匹配用户发券
 * 应用窗口（为了解决获取count重复问题，并且能知道何时送数结束）
 * 使用函数决定
 *
 * author:xuyaokun_kzx
 * date:2022/2/10
 * desc:
*/
public class UserCouponMatchingTask3 {

    private final static Logger LOGGER = LoggerFactory.getLogger(UserCouponMatchingTask3.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = KafkaConsumerProducerConfig.getFlinkKafkaConsumer("coupon-input");
        //添加输入源
        DataStream<String> stream = env.addSource(flinkKafkaConsumer);
        SingleOutputStreamOperator<CouponInputTableVO> stream2 = stream.map(new MapFunction<String, CouponInputMsg>() {
            @Override
            public CouponInputMsg map(String value) throws Exception {
                System.out.println("输入端入参：" + value);
                CouponInputMsg flinkTopicMsg = JacksonUtils.toJavaObject(value, CouponInputMsg.class);
                return flinkTopicMsg;
            }
        }).map(new MapFunction<CouponInputMsg, CouponInputTableVO>() {
            @Override
            public CouponInputTableVO map(CouponInputMsg value) throws Exception {
                CouponInputTableVO couponInputTableVO = new CouponInputTableVO();
                couponInputTableVO.setMessageType(value.getMESSAGE_TYPE());
                couponInputTableVO.setCouponId(value.getCOUPON_ID());
                couponInputTableVO.setStoreId(value.getSTORE_ID());
                couponInputTableVO.setStoreRange(value.getSTORE_RANGE());
                couponInputTableVO.setStoreLatitude(value.getSTORE_LATITUDE());
                couponInputTableVO.setStoreLongitude(value.getSTORE_LONGITUDE());
                couponInputTableVO.setUserNum(value.getUSER_NUM());
                System.out.println("分流前转换得到内容：" + JacksonUtils.toJSONString(couponInputTableVO));
                return couponInputTableVO;
            }
        });

        //开始分流，按照消息类型分流
        OutputTag<CouponInputTableVO> itemTypeTag = new OutputTag<CouponInputTableVO>("itemType") {};
        OutputTag<CouponInputTableVO> itemTypeTag2 = new OutputTag<CouponInputTableVO>("itemType2") {};
        OutputTag<CouponInputTableVO> countTypeTag = new OutputTag<CouponInputTableVO>("countType") {};

        SingleOutputStreamOperator<CouponInputTableVO> splitStream = stream2.process(new ProcessFunction<CouponInputTableVO, CouponInputTableVO>() {
            @Override
            public void processElement(CouponInputTableVO value, Context context, Collector<CouponInputTableVO> out) throws Exception {
                if ("01".equals(value.getMessageType())) {
                    context.output(itemTypeTag, value);
                    context.output(itemTypeTag2, value);
                } else if ("02".equals(value.getMessageType())) {
                    context.output(countTypeTag, value);
                }
            }
        });

        //得到划分后的流
        //消息类型01对应的流
        DataStream<CouponInputTableVO> itemTypeStream = splitStream.getSideOutput(itemTypeTag);
//        itemTypeStream.print();
        DataStream<CouponInputTableVO> itemTypeStream2 = splitStream.getSideOutput(itemTypeTag2);

        //消息类型02对应的流
        DataStream<CouponInputTableVO> countTypeStream = splitStream.getSideOutput(countTypeTag);
//        countTypeStream.print();

        //针对01类型的处理
        //通过流得到kafka table
        //将流转成表
        Table inputTable = tableEnv.fromDataStream(itemTypeStream, $("couponId"), $("storeId"),
                $("storeRange"), $("storeLongitude"), $("storeLatitude"),
                $("userNum"));
        Table inputTable2 = tableEnv.fromDataStream(itemTypeStream2, $("couponId"), $("storeId"),
                $("storeRange"), $("storeLongitude"), $("storeLatitude"),
                $("userNum"));
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

        // 注册函数
        tableEnv.createTemporarySystemFunction("lat", LatFunction.class);
        tableEnv.createTemporarySystemFunction("lng", LngFunction.class);
        tableEnv.createTemporarySystemFunction("curMinute", CurrentMinute.class);

        //模拟表名：user_location_partition
        //在这里需要将要用到的列筛选出来
        String queryHiveSql = "SELECT cert_type, cert_nbr, lat, lng, lat_night, lng_night, work_day, destination, partstart "
                + " FROM user_location_partition_info "
                + " WHERE partstart='20220210'" //带上分区信息(注意，这里的分区值必须用引号括起来)
                ;
        Table hiveTable = tableEnv.sqlQuery(queryHiveSql);

        //两个表进行连接join
        Table joinResTable = inputTable.join(hiveTable);
        //调试，查看表连接之后的内容
//        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(joinResTable, Row.class);
//        retractStream.print();

//        boolean queryCount = true;
        boolean queryCount = false;
        if (queryCount){
            Table hiveTable2 = tableEnv.sqlQuery(queryHiveSql);
            Table joinResTable2 = inputTable2.join(hiveTable2);

            Table countResultTable = joinResTable2
                    .select($("couponId"), $("storeId"), $("storeRange"), $("userNum"))
                    .where(" ROUND(6378.138 * 2 * ASIN(SQRT(\n" +
                            "POWER(SIN((CAST(storeLatitude as double) * PI() / 180 - CAST(lat as double) * PI() / 180) / 2), 2)\n" +
                            "+ COS(CAST(storeLatitude as double) * PI() / 180) * COS(CAST(lat as double) * PI() / 180) * \n" +
                            "POWER(SIN((CAST(storeLongitude as double) * PI() / 180 - CAST(lng as double) * PI() / 180) / 2), 2)\n" +
                            ")) * 1000) < storeRange")
                    .limit(10000)
                    .groupBy($("couponId"), $("storeId"), $("storeRange"), $("userNum"))
                    .select($("couponId"), $("storeId"), $("storeRange"), $("userNum"), $("couponId").count().as("cnt"));

            //针对连接后的表进行查询
            //查总数（查完总数，假如hive里的数据仍会动态变，就会导致数据不准，例如一开始查出总数是100，数据增量进来，给101个用户发了券）
        /*
            distinct
            为什么会查出多条记录？重新创建table出来join也一样查出多条记录，这个group by怎么会查出多条记录呢？
         */
//            Table countResultTable = tableEnv.sqlQuery(
//                    "SELECT  t.couponId, t.storeId, t.storeRange, t.userNum, count(1) as userCount from (" +
//                            "SELECT couponId, storeId, storeRange, userNum " +
//                            "FROM " + joinResTable2 +
//                            " where ROUND(6378.138 * 2 * ASIN(SQRT(\n" +
//                            "POWER(SIN((CAST(storeLatitude as double) * PI() / 180 - CAST(lat as double) * PI() / 180) / 2), 2)\n" +
//                            "+ COS(CAST(storeLatitude as double) * PI() / 180) * COS(CAST(lat as double) * PI() / 180) * \n" +
//                            "POWER(SIN((CAST(storeLongitude as double) * PI() / 180 - CAST(lng as double) * PI() / 180) / 2), 2)\n" +
//                            ")) * 1000) < storeRange limit 10000" +
//                            ") t group by t.couponId, t.storeId, t.storeRange, t.userNum"
//            );

            //下面这条SQL不可取
//            Table countResultTable = tableEnv.sqlQuery(
//                    "SELECT  count(1) as userCount from (" +
//                            "SELECT couponId, storeId, storeRange, userNum " +
//                            "FROM " + joinResTable2 +
//                            " where ROUND(6378.138 * 2 * ASIN(SQRT(\n" +
//                            "POWER(SIN((CAST(storeLatitude as double) * PI() / 180 - CAST(lat as double) * PI() / 180) / 2), 2)\n" +
//                            "+ COS(CAST(storeLatitude as double) * PI() / 180) * COS(CAST(lat as double) * PI() / 180) * \n" +
//                            "POWER(SIN((CAST(storeLongitude as double) * PI() / 180 - CAST(lng as double) * PI() / 180) / 2), 2)\n" +
//                            ")) * 1000) < storeRange limit 10000" +
//                            ") t "
//            );
            DataStream<Tuple2<Boolean, Row>> countResultStream = tableEnv.toRetractStream(countResultTable, Row.class);
            countResultStream.print();

            SingleOutputStreamOperator<CouponOutputMsg> countResultOutputStream = countResultStream.map(new MapFunction<Tuple2<Boolean, Row>, CouponOutputMsg>() {
                @Override
                public CouponOutputMsg map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {

                    //cert_type, cert_nbr, couponId, storeId, storeRange, userNum
                    String rowToString = booleanRowTuple2.f1.toString();
                    //输出结果：rowToString: 4,44444,347caf17-7f6d-41b7-9ba5-b3b1f49f5ea6,888999,500,null  并不是按照json输出
                    System.out.println("count rowToString: " + rowToString);
                    CouponOutputMsg couponOutputMsg = new CouponOutputMsg();
                    couponOutputMsg.setMESSAGE_TYPE("01");
                    couponOutputMsg.setSERIAL_NO(UUID.randomUUID().toString());
//                couponOutputMsg.setCOUPON_ID((String) booleanRowTuple2.f1.getField(0));
//                couponOutputMsg.setSTORE_ID((String) booleanRowTuple2.f1.getField(1));
//                couponOutputMsg.setSTORE_RANGE((String) booleanRowTuple2.f1.getField(2));
//                //实际筛选客户总数
//                couponOutputMsg.setCOUPON_SEND_NUM(String.valueOf(booleanRowTuple2.f1.getField(4)));

//                couponOutputMsg.setCOUPON_ID((String) booleanRowTuple2.f1.getField(0));
//                couponOutputMsg.setSTORE_ID((String) booleanRowTuple2.f1.getField(1));
//                couponOutputMsg.setSTORE_RANGE((String) booleanRowTuple2.f1.getField(2));
//                //实际筛选客户总数
                    couponOutputMsg.setCOUPON_SEND_NUM(String.valueOf(booleanRowTuple2.f1.getField(0)));
                    return couponOutputMsg;
                }
            });
//        countResultOutputStream.print();
        }


        //执行经纬度比较SQL
        //查出所有符合条件的行(多行)
        //列名的选择需要根据时间段动态变！自定义一个函数，提供这个列名
        Table itemResultTable = tableEnv.sqlQuery(
                "SELECT cert_type, cert_nbr, couponId, storeId, storeRange, userNum " +
                        "FROM " + joinResTable +
                        " where ROUND(6378.138 * 2 * ASIN(SQRT(\n" +
                        "POWER(SIN((CAST(storeLatitude as double) * PI() / 180 - CAST(" +
                        "(CASE lat(date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss'))\n" +
                        "WHEN '0' THEN lat\n" +
                        "WHEN '1' THEN lat_night\n" +
                        "ELSE lat_night \n" +
                        "END) " +
                        " as double) * PI() / 180) / 2), 2)\n" +
                        "+ COS(CAST(storeLatitude as double) * PI() / 180) * COS(CAST(" +
                        "(CASE lat(date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss'))\n" +
                        "WHEN '0' THEN lat\n" +
                        "WHEN '1' THEN lat_night\n" +
                        "ELSE lat_night \n" +
                        "END) " +
                        " as double) * PI() / 180) * \n" +
                        "POWER(SIN((CAST(storeLongitude as double) * PI() / 180 - CAST(" +
                        "(CASE lng(date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss'))\n" +
                        "WHEN '0' THEN lng\n" +
                        "WHEN '1' THEN lng_night\n" +
                        "ELSE lng_night \n" +
                        "END) " +
                        " as double) * PI() / 180) / 2), 2)\n" +
                        ")) * 1000) < storeRange limit 10000"  //limit的取值如何动态变？
//                        + " and " +
//                        " partstart='20220210'"
        );


        //表转成流
        DataStream<Tuple2<Boolean, Row>> itemResultStream = tableEnv.toRetractStream(itemResultTable, Row.class);

        //结果的处理,转换成kafka输出格式
        SingleOutputStreamOperator<CouponOutputMsg> itemResultOutputStream = itemResultStream.map(new MapFunction<Tuple2<Boolean, Row>, CouponOutputMsg>() {
            @Override
            public CouponOutputMsg map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {

                //cert_type, cert_nbr, couponId, storeId, storeRange, userNum
                String rowToString = booleanRowTuple2.f1.toString();
                //输出结果：rowToString: 4,44444,347caf17-7f6d-41b7-9ba5-b3b1f49f5ea6,888999,500,null  并不是按照json输出
//                System.out.println("rowToString: " + rowToString);
                CouponOutputMsg couponOutputMsg = new CouponOutputMsg();
                couponOutputMsg.setMESSAGE_TYPE("02");
                couponOutputMsg.setSERIAL_NO(UUID.randomUUID().toString());
                couponOutputMsg.setID_TYPE((String) booleanRowTuple2.f1.getField(0));
                couponOutputMsg.setID_NUMBER((String) booleanRowTuple2.f1.getField(1));
                couponOutputMsg.setCOUPON_ID((String) booleanRowTuple2.f1.getField(2));
                couponOutputMsg.setSTORE_ID((String) booleanRowTuple2.f1.getField(3));
                couponOutputMsg.setSTORE_RANGE((String) booleanRowTuple2.f1.getField(4));
                couponOutputMsg.setCOUPON_SEND_NUM((String) booleanRowTuple2.f1.getField(5));
                return couponOutputMsg;
            }
        });
        itemResultOutputStream.print("itemResultOutputStream");

        //输出源
        FlinkKafkaProducer flinkKafkaProducer = FlinkKafkaConfig.getFlinkKafkaProducer("coupon-output");
//        itemResultOutputStream.addSink(flinkKafkaProducer);

        System.out.println("开始执行UserCouponMatchingTask");
        env.execute();
    }

    public static class CurrentMinute extends ScalarFunction {

        public String eval(String dateString) {

            //模拟，分钟数是偶数，返回0，奇数返回1
            String mm = DateUtils.toStr(new Date(), "mm");
            return mm;
        }

    }

    public static class LatFunction extends ScalarFunction {

        public String eval(String dateString) {

            return matchTimePeriod(dateString);
        }

    }

    public static class LngFunction extends ScalarFunction {

        public String eval(String dateString) {

            return matchTimePeriod(dateString);
        }
    }

    private static String matchTimePeriod(String dateString){

//        return Integer.parseInt(dateString) % 2 == 0 ? "0" : "1";

        Date date = DateUtils.toDate(dateString, PATTERN_YYYY_MM_DD_HH_MM_SS);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        int minute = calendar.get(Calendar.MINUTE);
        return minute % 2 == 0 ? "0" : "1";
    }

}

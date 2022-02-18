package com.kunghsu.apache.flink.flinkkafka.task;

import com.kunghsu.common.utils.JacksonUtils;
import com.kunghsu.example.coupon.CouponInputMsg;
import com.kunghsu.example.coupon.table.CouponInputTableVO;
import com.kunghsu.example.coupon.KafkaConsumerProducerConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class WindowDemoTask {

    private static String JOB_NAME = "WindowDemoTask";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                System.out.println("分流前转换得到内容：" + JacksonUtils.toJSONString(couponInputTableVO));
                return couponInputTableVO;
            }
        });

//        stream2.keyBy().

        System.out.println("开始执行" + JOB_NAME);
        env.execute(JOB_NAME);
    }


}

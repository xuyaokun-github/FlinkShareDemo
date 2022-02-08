package com.kunghsu.apache.flink.flinkkafka.operators.map;

import com.kunghsu.apache.flink.flinkkafka.model.FlinkTopicDealResultMsg;
import com.kunghsu.common.utils.JacksonUtils;
import com.kunghsu.common.vo.ResultVo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class MoreKafkaSourceByJoinAndSessionWindowFlatMapFunction implements FlatMapFunction<FlinkTopicDealResultMsg, String> {

    @Override
    public void flatMap(FlinkTopicDealResultMsg value, Collector<String> out) throws Exception {

        if (StringUtils.isNotEmpty(value.getMsgId())){
            //说明得到正确的处理
            String newRes = JacksonUtils.toJSONString(value);
            //放到输出流
            out.collect(newRes);
        }else {
            out.collect(JacksonUtils.toJSONString(ResultVo.valueOfError("处理异常")));
        }

    }


}

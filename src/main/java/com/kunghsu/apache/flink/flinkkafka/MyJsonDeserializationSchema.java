package com.kunghsu.apache.flink.flinkkafka;

import com.kunghsu.apache.flink.flinkkafka.model.FlinkTopicMsg;
import com.kunghsu.common.utils.JacksonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class MyJsonDeserializationSchema implements DeserializationSchema<FlinkTopicMsg> {

    @Override
    public FlinkTopicMsg deserialize(byte[] message) throws IOException {
        String string = new String(message, "UTF-8");
        return JacksonUtils.toJavaObject(string, FlinkTopicMsg.class);
    }

    @Override
    public boolean isEndOfStream(FlinkTopicMsg nextElement) {
        return false;
    }

    @Override
    public TypeInformation<FlinkTopicMsg> getProducedType() {
        return BasicTypeInfo.getInfoFor(FlinkTopicMsg.class);
    }
}

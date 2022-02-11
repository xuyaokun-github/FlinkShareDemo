package com.kunghsu.example.coupon;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;

public class KafkaConsumerProducerConfig {

    public static FlinkKafkaConsumer getFlinkKafkaConsumer(String topic){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flinksharedemo");
        properties.setProperty("max.poll.records", "1");

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
        myConsumer.setStartFromGroupOffsets(); // 默认的方法（消费过的不会再被消费）
//        myConsumer.setStartFromEarliest();     // 尽可能从最早的记录开始(该消费者组拉取过的，还是会再次消费)
//        myConsumer.setStartFromLatest();       // 从最新的记录开始
//        myConsumer.setStartFromTimestamp(...); // 从指定的时间开始（毫秒）
        return myConsumer;

    }

    public static FlinkKafkaProducer getFlinkKafkaProducer(String topic){
        //生产者配置
        Properties produceProperties = new Properties();
        produceProperties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                topic,                  // 目标 topic
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), // 序列化 schema
                produceProperties,                  // producer 配置
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); //容错
        return myProducer;
    }


}

package com.kunghsu.apache.flink.checkpoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RestoreCheckpointExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 配置Checkpoint
        env.enableCheckpointing(1000);
        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/flink/checkpoint"));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //配置失败重启策略：失败后最多重启3次 每次重启间隔10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        DataStream source = env.socketTextStream("localhost", 9100, "\n")
                .name("MySourceFunction");
//        DataStream wordsCount = source.flatMap(new FlatMapFunction<String>() {
//            @Override
//            public void flatMap (String value, Collector out){
//                //失败信号
//                if (Objects.equals(value, "ERROR")) {
//                    throw new RuntimeException("custom error flag, restart application");
//                }
//                //拆分单词
//                for (String word : value.split("\\s")) {
//                    out.collect(Tuple2.of(word, 1));
//                }
//            }
//        }).name("MyFlatMapFunction");
//        DataStream windowCount = wordsCount
//                .keyBy(new KeySelector<String> () {
//            @Override
//            public String getKey (Tuple2 tuple) throws Exception {
//                return (String) tuple.f0;
//            }
//        }).sum(1).name("MySumFunction");
//        windowCount.print().setParallelism(1).name("MyPrintFunction");
        env.execute("RestoreCheckpointExample");
    }
    //
}


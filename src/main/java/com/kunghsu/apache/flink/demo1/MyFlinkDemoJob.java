package com.kunghsu.apache.flink.demo1;

import com.kunghsu.apache.flink.FlinkDemoJob;
import com.kunghsu.common.utils.JacksonUtils;

public class MyFlinkDemoJob implements FlinkDemoJob {

    @Override
    public void run(String[] args) {
        System.out.println("MyFlinkDemoJob running.............");
        System.out.println(JacksonUtils.toJSONString(args));
        while (true){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

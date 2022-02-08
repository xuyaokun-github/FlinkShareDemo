package com.kunghsu.apache.flink.tableapi.hive;

import java.util.Arrays;

/**
 * hive造数
 * author:xuyaokun_kzx
 * date:2022/1/6
 * desc:
*/
public class MakeTestData {

    private static String[] columns = new String[]{"id", "name", "work_day", "destination"};

    public static void main(String[] args) {

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 2; i++) {
            int finalI = i;
            StringBuilder builder2 = new StringBuilder();
            Arrays.stream(columns).forEach(item->{
                if (item.equals("id")){
                    builder2.append(String.valueOf(finalI));
                    builder2.append("\001");
                }else if (item.contains("day")){
                    builder2.append("");
                    builder2.append("\001");
                }else {
                    builder2.append(item + finalI);
                    builder2.append("\001");
                }
            });
            String result = builder2.toString();
            builder.append(result.substring(0, result.length() - 1));
            builder.append("\n");
        }
        System.out.println(builder.toString());
    }
}

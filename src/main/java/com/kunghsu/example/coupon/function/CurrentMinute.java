package com.kunghsu.example.coupon.function;

import com.kunghsu.common.utils.DateUtils;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Date;

public class CurrentMinute extends ScalarFunction {

    public String eval(String dateString) {

        //模拟，分钟数是偶数，返回0，奇数返回1
        String mm = DateUtils.toStr(new Date(), "mm");
        return mm;
    }

}

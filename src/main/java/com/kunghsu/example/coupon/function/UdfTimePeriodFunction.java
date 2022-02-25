package com.kunghsu.example.coupon.function;

import com.kunghsu.common.utils.DateUtils;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Calendar;
import java.util.Date;

import static com.kunghsu.common.utils.DateUtils.PATTERN_YYYY_MM_DD_HH_MM_SS;

public class UdfTimePeriodFunction extends ScalarFunction {

    public String eval(String param) {

        String dateString = DateUtils.now();
        return matchTimePeriod(dateString);
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

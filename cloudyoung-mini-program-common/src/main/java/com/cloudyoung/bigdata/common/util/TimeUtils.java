package com.cloudyoung.bigdata.common.util;

import org.apache.commons.lang.time.DateUtils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtils {
    public static String format(Date date, String pattern) {
        return getSdf(pattern).format(date);
    }

    public static SimpleDateFormat getSdf(final String pattern) {
        SimpleDateFormat tl = new SimpleDateFormat(pattern);
        return tl;
    }

    public static String getHour(Date time, int n, String pattern) {
        time = DateUtils.setSeconds(time, 0);
        time = DateUtils.setMinutes(time, 0);
        time = DateUtils.addHours(time, n);
        return format(time, pattern);
    }

    public static String getDay(Date time, int n, String pattern) {
        time = DateUtils.setSeconds(time, 0);
        time = DateUtils.setMinutes(time, 0);
        time = DateUtils.addHours(time, 0);
        time = DateUtils.addDays(time, n);
        return format(time, pattern);
    }

    public static Date strToDate(String str, String pattern) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.parse(str);
    }

    public static String numberToDataStr(long ms, String pattern) {
        String str = "";
        if (ms > 0) {
            SimpleDateFormat sdf = new SimpleDateFormat(pattern);
            try {
                str = sdf.format(ms);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return str;
    }

}

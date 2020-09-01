package com.atguigu.publisher.util;

import org.apache.commons.lang3.time.DateUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/1 10:50
 */
public class DateUtil {

    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    public static SimpleDateFormat newFormat = new SimpleDateFormat("yyyyMMdd");

    public static String getYD(String date) {
        try {
            Date yesDate = DateUtils.addDays(format.parse(date), -1);
            return format.format(yesDate);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期转换错误");
        }
    }

    public static String getESDate(String date) {
        try {
            return newFormat.format(DateUtil.format.parse(date));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("日期转换错误");
        }
    }

}

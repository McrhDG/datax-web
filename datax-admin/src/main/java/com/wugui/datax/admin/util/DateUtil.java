package com.wugui.datax.admin.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日志工具类
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2021/7/1 10:02
 */
public class DateUtil {

    private DateUtil() {
    }

    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final SimpleDateFormat DEFAULT_FORMAT = new SimpleDateFormat(DATE_FORMAT);

    public static String format() {
        return DEFAULT_FORMAT.format(new Date());
    }


    public static String format(long time) {
        return DEFAULT_FORMAT.format(new Date(time));
    }
}

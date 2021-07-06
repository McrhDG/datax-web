package com.wugui.datax.admin.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

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

    private final static DateTimeFormatter DF = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String format() {
        return LocalDateTime.now().format(DF);
    }


    public static String format(long time) {
        return Instant.ofEpochMilli(time).atZone(ZoneOffset.ofHours(8)).toLocalDateTime().format(DF);
    }
}

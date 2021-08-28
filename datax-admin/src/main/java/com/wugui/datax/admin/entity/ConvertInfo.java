package com.wugui.datax.admin.entity;

import lombok.Data;
import lombok.Getter;

import java.util.Map;

/**
 * 同步装换信息
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2021/6/23 18:56
 */
@Data
public class ConvertInfo {

    /**
     * 构造
     * @param tableName
     * @param tableColumns
     * @param writeUrl
     */
    public ConvertInfo(int jobId, String tableName, Map<String, ToColumn> tableColumns, String writeUrl, long initTimestamp) {
        this.jobId = jobId;
        this.tableName = tableName;
        this.tableColumns = tableColumns;
        this.writeUrl = writeUrl;
        this.initTimestamp = initTimestamp;
    }

    /**
     * 任务id
     */
    private final int jobId;

    /**
     * 装换后表名
     */
    private final String tableName;

    /**
     * 列名装换
     */
    private final Map<String, ToColumn> tableColumns;

    /**
     * 写入放连接url
     */
    private final String writeUrl;

    /**
     * 插入语句
     */
    private String insertSql;

    /**
     * 删除语句
     */
    private String deleteSql;

    /**
     * 更新条件语句
     */
    private String conditionSql;

    /**
     * 开始同步时间
     */
    private long initTimestamp;

    @Getter
    public static class ToColumn {

        public ToColumn(String name) {
            this.name = name;
        }

        public ToColumn(String name, String fromType, String splitter) {
            this.name = name;
            this.fromType = fromType;
            this.splitter = splitter;
        }

        /**
         * 列名
         */
        private String name;

        /**
         * 来源列类型
         */
        private String fromType;

        /**
         * 分隔符
         */
        private String splitter;
    }
}

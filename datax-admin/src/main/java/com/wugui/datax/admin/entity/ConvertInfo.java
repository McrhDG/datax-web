package com.wugui.datax.admin.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

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
    public ConvertInfo(int jobId, String tableName, Map<String, String> tableColumns, String writeUrl, long initTimestamp) {
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
    private final Map<String, String> tableColumns;

    /**
     * 写入放连接url
     */
    private final String writeUrl;

    /**
     * 插入语句
     */
    private String insertSql;

    /**
     * 插入语句
     */
    private String replaceSql;

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
}

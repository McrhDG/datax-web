package com.wugui.datax.admin.entity;

import lombok.Builder;
import lombok.Data;

import java.util.Date;

/**
 * 增量同步等待表
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2021/6/25 13:48
 */
@Data
@Builder
public class IncrementSyncWaiting {

    /**
     * 主键
     */
    private int id;

    /**
     * 任务id
     */
    private int jobId;

    /**
     * 同步类型mongo/mysql
     */
    private String type;

    /**
     * 操作类型INSERT/UPDATE/DELETE/REPLACE
     */
    private String operationType;

    /**
     * 需要同步的内容
     */
    private String content;

    /**
     * 主键值
     */
    private String idValue;

    /**
     * 条件
     */
    private String condition;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 更新时间
     */
    private Date updateTime;
}

package com.wugui.datax.admin.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 列值
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2021/7/1 10:20
 */
@Data
@NoArgsConstructor
public class ColumnValue {

    /**
     * 构造
     * @param column
     */
    public ColumnValue(CanalEntry.Column column) {
        if (!column.getIsNull()) {
            this.value = column.getValue();
        }
        this.sqlType = column.getSqlType();
    }

    /**
     * 列值
     */
    private String value;

    /**
     * sql类型
     */
    private Integer sqlType;
}

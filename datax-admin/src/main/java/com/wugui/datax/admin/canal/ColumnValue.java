package com.wugui.datax.admin.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Types;

/**
 * 列值
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2021/7/1 10:20
 */
@Data
@NoArgsConstructor
@Slf4j
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
        if(Types.NUMERIC == column.getSqlType() || Types.DECIMAL == column.getSqlType()) {
            String mysqlType = column.getMysqlType();
            if(mysqlType.contains(",")) {
                String point = mysqlType.substring(mysqlType.indexOf(',')+1, mysqlType.indexOf(')'));
                try {
                    scale = Integer.parseInt(point);
                } catch (NumberFormatException e) {
                    log.error("point length:{}, change point length error", point);
                }
            }
        }
    }

    /**
     * 列值
     */
    private String value;

    /**
     * sql类型
     */
    private Integer sqlType;

    /**
     * 小数点长度
     */
    private int scale;
}

package com.wugui.datax.admin.mongo;

import com.mongodb.client.model.changestream.OperationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 *
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2022/5/7 15:02
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MongoJobInfo {

    /**
     * 主键描述
     */
    private String idValue;

    /**
     * 类型
     */
    private OperationType eventType;

    /**
     * 更新的字段
     */
    private Map<String, Object> updateColumns;

    /**
     * 条件字段
     */
    private String id;
}

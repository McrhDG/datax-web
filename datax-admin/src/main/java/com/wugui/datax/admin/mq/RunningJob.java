package com.wugui.datax.admin.mq;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 正在运行中任务
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2021/6/25 11:15
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RunningJob {

    /**
     * 任务Id
     */
    private Integer jobId;

    /**
     * 是否添加: true-add,false-remove
     */
    private Boolean isAdd;
}

package com.wugui.datax.admin.mq;

import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.core.util.IncrementUtil;
import com.wugui.datax.admin.entity.JobInfo;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * 分布式端点
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2021/6/23 11:31
 */
@Component
@RabbitListener(queues = "#{endpointSyncQueue.name}")
public class EndpointSyncListener {

    /**
     * 接收
     * @param jobInfo
     * @param sourceIp
     */
    @RabbitHandler
    public void process(@Payload JobInfo jobInfo, @Header String sourceIp, @Header String type) {
        if (!JobAdminConfig.getAdminConfig().getIp().equals(sourceIp)) {
            if (ProjectConstant.ACTION_TYPE.DELETE.val().equals(type)) {
                IncrementUtil.removeTask(jobInfo, true);
            } else if (ProjectConstant.ACTION_TYPE.REMOVE.val().equals(type)) {
                IncrementUtil.removeTask(jobInfo, false);
            } else if (ProjectConstant.ACTION_TYPE.TRIGGER.val().equals(type)) {
                IncrementUtil.initIncrementData(jobInfo, true);
                IncrementUtil.addQueue(jobInfo);
            }
        }
    }

    /**
     * 接收
     * @param runningJob
     */
    @RabbitHandler
    public void process(@Payload RunningJob runningJob) {
        if (runningJob.getIsAdd()) {
            IncrementUtil.addRunningJob(runningJob.getJobId());
        } else {
            IncrementUtil.removeRunningJob(runningJob.getJobId());
        }
    }
}

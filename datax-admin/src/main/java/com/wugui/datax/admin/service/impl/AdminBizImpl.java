package com.wugui.datax.admin.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.wenwo.cloud.message.driven.producer.service.MessageProducerService;
import com.wugui.datatx.core.biz.AdminBiz;
import com.wugui.datatx.core.biz.model.HandleCallbackParam;
import com.wugui.datatx.core.biz.model.HandleProcessCallbackParam;
import com.wugui.datatx.core.biz.model.RegistryParam;
import com.wugui.datatx.core.biz.model.ReturnT;
import com.wugui.datatx.core.enums.IncrementTypeEnum;
import com.wugui.datatx.core.handler.IJobHandler;
import com.wugui.datax.admin.canal.CanalWorkThread;
import com.wugui.datax.admin.canal.ColumnValue;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.core.kill.KillJob;
import com.wugui.datax.admin.core.thread.JobTriggerPoolHelper;
import com.wugui.datax.admin.core.trigger.TriggerTypeEnum;
import com.wugui.datax.admin.core.util.I18nUtil;
import com.wugui.datax.admin.core.util.IncrementUtil;
import com.wugui.datax.admin.entity.ConvertInfo;
import com.wugui.datax.admin.entity.IncrementSyncWaiting;
import com.wugui.datax.admin.entity.JobInfo;
import com.wugui.datax.admin.entity.JobLog;
import com.wugui.datax.admin.mapper.IncrementSyncWaitingMapper;
import com.wugui.datax.admin.mapper.JobInfoMapper;
import com.wugui.datax.admin.mapper.JobLogMapper;
import com.wugui.datax.admin.mapper.JobRegistryMapper;
import com.wugui.datax.admin.mongo.MongoWatchWorkThread;
import com.wugui.datax.admin.mq.RunningJob;
import com.wugui.datax.admin.util.RedisLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author xuxueli 2017-07-27 21:54:20
 */
@Service
@DependsOn({"mongoWatchWork"})
public class AdminBizImpl implements AdminBiz {
    private static final Logger logger = LoggerFactory.getLogger(AdminBizImpl.class);

    @Resource
    public JobLogMapper jobLogMapper;
    @Resource
    private JobInfoMapper jobInfoMapper;
    @Resource
    private JobRegistryMapper jobRegistryMapper;

    @Resource
    private IncrementSyncWaitingMapper incrementSyncWaitingMapper;

    /** redis锁*/
    @Autowired
    private RedisLock redisLock;

    /**
     * 处理待运行数据
     */
    @PostConstruct
    public void process() {
        executeIncrementSyncWaitings();
    }

    /**
     * 执行增量等待方法
     */
    @Override
    public void executeIncrementSyncWaitings() {
        List<Integer> jobIds = incrementSyncWaitingMapper.findJobIds();
        if (CollectionUtils.isEmpty(jobIds)) {
            return;
        }
        for (Integer jobId : jobIds) {
            executeIncrementSyncWaiting(jobId);
        }
    }

    @Override
    public ReturnT<String> callback(List<HandleCallbackParam> callbackParamList) {
        for (HandleCallbackParam handleCallbackParam : callbackParamList) {
            ReturnT<String> callbackResult = callback(handleCallbackParam);
            logger.debug(">>>>>>>>> JobApiController.callback {}, handleCallbackParam={}, callbackResult={}",
                    (callbackResult.getCode() == IJobHandler.SUCCESS.getCode() ? "success" : "fail"), handleCallbackParam, callbackResult);
        }

        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> processCallback(List<HandleProcessCallbackParam> callbackParamList) {
        for (HandleProcessCallbackParam handleProcessCallbackParam : callbackParamList) {
            ReturnT<String> callbackResult = processCallback(handleProcessCallbackParam);
            logger.debug(">>>>>>>>> JobApiController.processCallback {}, handleCallbackParam={}, callbackResult={}",
                    (callbackResult.getCode() == IJobHandler.SUCCESS.getCode() ? "success" : "fail"), handleProcessCallbackParam, callbackResult);
        }
        return ReturnT.SUCCESS;
    }

    private ReturnT<String> processCallback(HandleProcessCallbackParam handleProcessCallbackParam) {
        int result = jobLogMapper.updateProcessId(handleProcessCallbackParam.getLogId(), handleProcessCallbackParam.getProcessId());
        return result > 0 ? ReturnT.FAIL : ReturnT.SUCCESS;
    }

    /**
     * 执行增量同步等待任务
     * @param jobId
     */
    private void executeIncrementSyncWaiting(int jobId) {
        if (IncrementUtil.isRunningJob(jobId)) {
            return;
        }
        String lock = String.format(ProjectConstant.INCREMENT_WAIT_JOB_LOCK, jobId);
        if (redisLock.tryLock(lock, ProjectConstant.LOCK_TIMEOUT_300)) {
            try {
                List<IncrementSyncWaiting> incrementSyncWaitings = incrementSyncWaitingMapper.loadByJobId(jobId);
                if (CollectionUtils.isEmpty(incrementSyncWaitings)) {
                    return;
                }
                ConvertInfo convertInfo = IncrementUtil.getConvertInfo(jobId);
                if (convertInfo == null) {
                    logger.info("jobId:{}, 不存在读写装换关系", jobId);
                    incrementSyncWaitingMapper.deleteByJobId(jobId);
                    return;
                }

                for (IncrementSyncWaiting incrementSyncWaiting : incrementSyncWaitings) {
                    String operationType = incrementSyncWaiting.getOperationType();
                    String content = incrementSyncWaiting.getContent();
                    String condition = incrementSyncWaiting.getCondition();
                    String type = incrementSyncWaiting.getType();
                    String idValue = incrementSyncWaiting.getIdValue();
                    try {
                        switch (operationType) {
                            case "INSERT":
                                if (ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH.val().equals(type)) {
                                    Map<String, Object> data = JSON.parseObject(content);
                                    MongoWatchWorkThread.insert(convertInfo, data);
                                } else if (ProjectConstant.INCREMENT_SYNC_TYPE.CANAL.val().equals(type)) {
                                    Map<String, ColumnValue> insertMap = JSON.parseObject(content, new TypeReference<Map<String, ColumnValue>>() {});
                                    CanalWorkThread.insert(convertInfo, insertMap);
                                }
                                break;
                            case "UPDATE":
                            case "REPLACE":
                                if (ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH.val().equals(type)) {
                                    Map<String, Object> data = JSON.parseObject(content);
                                    MongoWatchWorkThread.update(convertInfo, data, condition);
                                } else if (ProjectConstant.INCREMENT_SYNC_TYPE.CANAL.val().equals(type)) {
                                    Map<String, ColumnValue> updateMap = JSON.parseObject(content, new TypeReference<Map<String, ColumnValue>>() {});
                                    Map<String, ColumnValue> conditionMap = JSON.parseObject(condition, new TypeReference<Map<String, ColumnValue>>() {});
                                    CanalWorkThread.update(convertInfo, updateMap, conditionMap);
                                }
                                break;
                            case "DELETE":
                                if (ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH.val().equals(type)) {
                                    MongoWatchWorkThread.delete(convertInfo, condition);
                                } else if (ProjectConstant.INCREMENT_SYNC_TYPE.CANAL.val().equals(type)) {
                                    Map<String, ColumnValue> conditionMap = JSON.parseObject(condition, new TypeReference<Map<String, ColumnValue>>() {});
                                    CanalWorkThread.delete(convertInfo, conditionMap);
                                }
                                break;
                            default:
                                logger.info("operation:{} not support, jobId:{}", operationType, jobId);
                        }
                        incrementSyncWaitingMapper.deleteByOperation(jobId, idValue, operationType);
                    } catch (Exception e) {
                        logger.error("jobId:[},isValue:{}, execute error:{}",jobId, idValue, e.getMessage());
                    }
                }
            } finally {
                redisLock.unlock(lock);
            }
        }
    }

    private ReturnT<String> callback(HandleCallbackParam handleCallbackParam) {
        // valid log item
        JobLog log = jobLogMapper.load(handleCallbackParam.getLogId());
        if (log == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "log item not found.");
        }
        if (log.getHandleCode() > 0) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "log repeate callback.");     // avoid repeat callback, trigger child job etc
        }

        //执行等待增量任务
        executeIncrementSyncWaiting(log.getJobId());
        //同步到其他端点
        MessageProducerService messageProducerService = JobAdminConfig.getAdminConfig().getMessageProducerService();
        messageProducerService.sendMsg(new RunningJob(log.getJobId(), false), ProjectConstant.ENDPOINT_SYNC_ROUTING_KEY);

        // trigger success, to trigger child job
        String callbackMsg = null;
        int resultCode = handleCallbackParam.getExecuteResult().getCode();

        if (IJobHandler.SUCCESS.getCode() == resultCode) {

            JobInfo jobInfo = jobInfoMapper.loadById(log.getJobId());

            updateIncrementParam(log, jobInfo.getIncrementType());

            if (jobInfo != null && jobInfo.getChildJobId() != null && jobInfo.getChildJobId().trim().length() > 0) {
                callbackMsg = "<br><br><span style=\"color:#00c0ef;\" > >>>>>>>>>>>" + I18nUtil.getString("jobconf_trigger_child_run") + "<<<<<<<<<<< </span><br>";

                String[] childJobIds = jobInfo.getChildJobId().split(",");
                for (int i = 0; i < childJobIds.length; i++) {
                    int childJobId = (childJobIds[i] != null && childJobIds[i].trim().length() > 0 && isNumeric(childJobIds[i])) ? Integer.valueOf(childJobIds[i]) : -1;
                    if (childJobId > 0) {

                        JobTriggerPoolHelper.trigger(childJobId, TriggerTypeEnum.PARENT, -1, null, null);
                        ReturnT<String> triggerChildResult = ReturnT.SUCCESS;

                        // add msg
                        callbackMsg += MessageFormat.format(I18nUtil.getString("jobconf_callback_child_msg1"),
                                (i + 1),
                                childJobIds.length,
                                childJobIds[i],
                                (triggerChildResult.getCode() == ReturnT.SUCCESS_CODE ? I18nUtil.getString("system_success") : I18nUtil.getString("system_fail")),
                                triggerChildResult.getMsg());
                    } else {
                        callbackMsg += MessageFormat.format(I18nUtil.getString("jobconf_callback_child_msg2"),
                                (i + 1),
                                childJobIds.length,
                                childJobIds[i]);
                    }
                }

            }
        }

        //kill execution timeout DataX process
        if (!StringUtils.isEmpty(log.getProcessId()) && IJobHandler.FAIL_TIMEOUT.getCode() == resultCode) {
            KillJob.trigger(log.getId(), log.getTriggerTime(), log.getExecutorAddress(), log.getProcessId());
        }

        // handle msg
        StringBuffer handleMsg = new StringBuffer();
        if (log.getHandleMsg() != null) {
            handleMsg.append(log.getHandleMsg()).append("<br>");
        }
        if (handleCallbackParam.getExecuteResult().getMsg() != null) {
            handleMsg.append(handleCallbackParam.getExecuteResult().getMsg());
        }
        if (callbackMsg != null) {
            handleMsg.append(callbackMsg);
        }

        if (handleMsg.length() > 15000) {
            handleMsg = new StringBuffer(handleMsg.substring(0, 15000));  // text最大64kb 避免长度过长
        }

        // success, save log
        log.setHandleTime(new Date());
        log.setHandleCode(resultCode);
        log.setHandleMsg(handleMsg.toString());

        jobLogMapper.updateHandleInfo(log);
        jobInfoMapper.updateLastHandleCode(log.getJobId(), resultCode);

        return ReturnT.SUCCESS;
    }

    private void updateIncrementParam(JobLog log, Integer incrementType) {
        if (IncrementTypeEnum.ID.getCode() == incrementType) {
            jobInfoMapper.incrementIdUpdate(log.getJobId(),log.getMaxId());
        } else if (IncrementTypeEnum.TIME.getCode() == incrementType) {
            jobInfoMapper.incrementTimeUpdate(log.getJobId(), log.getTriggerTime());
        }
    }

    private boolean isNumeric(String str) {
        try {
            Integer.valueOf(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    public ReturnT<String> registry(RegistryParam registryParam) {

        // valid
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }

        int ret = jobRegistryMapper.registryUpdate(registryParam.getRegistryGroup(), registryParam.getRegistryKey(),
                registryParam.getRegistryValue(), registryParam.getCpuUsage(), registryParam.getMemoryUsage(), registryParam.getLoadAverage(), new Date());
        if (ret < 1) {
            jobRegistryMapper.registrySave(registryParam.getRegistryGroup(), registryParam.getRegistryKey(),
                    registryParam.getRegistryValue(), registryParam.getCpuUsage(), registryParam.getMemoryUsage(), registryParam.getLoadAverage(), new Date());

            // fresh
            freshGroupRegistryInfo(registryParam);
        }
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> registryRemove(RegistryParam registryParam) {

        // valid
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }

        int ret = jobRegistryMapper.registryDelete(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue());
        if (ret > 0) {

            // fresh
            freshGroupRegistryInfo(registryParam);
        }
        return ReturnT.SUCCESS;
    }

    private void freshGroupRegistryInfo(RegistryParam registryParam) {
        // Under consideration, prevent affecting core tables
    }

}

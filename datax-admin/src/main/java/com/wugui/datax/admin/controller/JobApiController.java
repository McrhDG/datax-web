package com.wugui.datax.admin.controller;

import com.wugui.datatx.core.biz.AdminBiz;
import com.wugui.datatx.core.biz.model.HandleCallbackParam;
import com.wugui.datatx.core.biz.model.HandleProcessCallbackParam;
import com.wugui.datatx.core.biz.model.RegistryParam;
import com.wugui.datatx.core.biz.model.ReturnT;
import com.wugui.datatx.core.util.JobRemotingUtil;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.core.util.JacksonUtil;
import com.wugui.datax.admin.entity.IncrementSyncWaiting;
import com.wugui.datax.admin.mapper.IncrementSyncWaitingMapper;
import io.swagger.annotations.ApiOperation;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * Created by xuxueli on 17/5/10.
 */
@RestController
@RequestMapping("/api")
public class JobApiController {

    @Resource
    private AdminBiz adminBiz;

    @Resource
    private IncrementSyncWaitingMapper incrementSyncWaitingMapper;


    /**
     * 执行增量等待方法
     */
    @ApiOperation(value = "执行增量等待")
    @GetMapping("/incrementWait")
    public ReturnT<String> executeIncrementSyncWaitings() {
        adminBiz.executeIncrementSyncWaitings();
        return ReturnT.SUCCESS;
    }

    /**
     * 查询增量等待
     */
    @ApiOperation(value = "查询增量等待")
    @GetMapping("/incrementWait/findAll")
    public ReturnT<List<IncrementSyncWaiting>> findAllIncrementSyncWaitings() {
        return new ReturnT<>(incrementSyncWaitingMapper.findAll());
    }

    /**
     * 删除增量等待
     */
    @ApiOperation(value = "删除增量等待")
    @PostMapping("/incrementWait/delete")
    public ReturnT<String> deleteIncrementSyncWaitings(@RequestBody List<Long> ids) {
        if (!CollectionUtils.isEmpty(ids)) {
            for (Long id : ids) {
                incrementSyncWaitingMapper.delete(id);
            }
        }
        return ReturnT.SUCCESS;
    }

    /**
     * callback
     *
     * @param data
     * @return
     */
    @PostMapping("/callback")
    public ReturnT<String> callback(HttpServletRequest request, @RequestBody(required = false) String data) {
        // valid
        if (JobAdminConfig.getAdminConfig().getAccessToken()!=null
                && JobAdminConfig.getAdminConfig().getAccessToken().trim().length()>0
                && !JobAdminConfig.getAdminConfig().getAccessToken().equals(request.getHeader(JobRemotingUtil.XXL_RPC_ACCESS_TOKEN))) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "The access token is wrong.");
        }

        // param
        List<HandleCallbackParam> callbackParamList = null;
        try {
            data = data.replace(new String(Character.toChars(26)),"");
            callbackParamList = JacksonUtil.readValue(data, List.class, HandleCallbackParam.class);
        } catch (Exception e) { }
        if (callbackParamList==null || callbackParamList.size()==0) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "The request data invalid.");
        }

        // invoke
        return adminBiz.callback(callbackParamList);
    }

    /**
     * callback
     *
     * @param data
     * @return
     */
    @PostMapping("/processCallback")
    public ReturnT<String> processCallback(HttpServletRequest request, @RequestBody(required = false) String data) {
        // valid
        if (JobAdminConfig.getAdminConfig().getAccessToken()!=null
                && JobAdminConfig.getAdminConfig().getAccessToken().trim().length()>0
                && !JobAdminConfig.getAdminConfig().getAccessToken().equals(request.getHeader(JobRemotingUtil.XXL_RPC_ACCESS_TOKEN))) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "The access token is wrong.");
        }

        // param
        List<HandleProcessCallbackParam> callbackParamList = null;
        try {
            callbackParamList = JacksonUtil.readValue(data, List.class, HandleProcessCallbackParam.class);
        } catch (Exception e) { }
        if (callbackParamList==null || callbackParamList.size()==0) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "The request data invalid.");
        }

        // invoke
        return adminBiz.processCallback(callbackParamList);
    }



    /**
     * registry
     *
     * @param data
     * @return
     */
    @PostMapping("/registry")
    public ReturnT<String> registry(HttpServletRequest request, @RequestBody(required = false) String data) {
        // valid
        if (JobAdminConfig.getAdminConfig().getAccessToken()!=null
                && JobAdminConfig.getAdminConfig().getAccessToken().trim().length()>0
                && !JobAdminConfig.getAdminConfig().getAccessToken().equals(request.getHeader(JobRemotingUtil.XXL_RPC_ACCESS_TOKEN))) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "The access token is wrong.");
        }

        // param
        RegistryParam registryParam = null;
        try {
            registryParam = JacksonUtil.readValue(data, RegistryParam.class);
        } catch (Exception e) {}
        if (registryParam == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "The request data invalid.");
        }

        // invoke
        return adminBiz.registry(registryParam);
    }

    /**
     * registry remove
     *
     * @param data
     * @return
     */
    @PostMapping("/registryRemove")
    public ReturnT<String> registryRemove(HttpServletRequest request, @RequestBody(required = false) String data) {
        // valid
        if (JobAdminConfig.getAdminConfig().getAccessToken()!=null
                && JobAdminConfig.getAdminConfig().getAccessToken().trim().length()>0
                && !JobAdminConfig.getAdminConfig().getAccessToken().equals(request.getHeader(JobRemotingUtil.XXL_RPC_ACCESS_TOKEN))) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "The access token is wrong.");
        }

        // param
        RegistryParam registryParam = null;
        try {
            registryParam = JacksonUtil.readValue(data, RegistryParam.class);
        } catch (Exception e) {}
        if (registryParam == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "The request data invalid.");
        }

        // invoke
        return adminBiz.registryRemove(registryParam);
    }


}

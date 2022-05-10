package com.wugui.datax.admin.canal;

import cn.hutool.core.collection.CollectionUtil;
import com.wenwo.cloud.message.driven.producer.service.MessageProducerService;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.util.IncrementUtil;
import com.wugui.datax.admin.entity.JobInfo;
import com.wugui.datax.admin.mapper.JobInfoMapper;
import com.wugui.datax.admin.util.SpringContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Doctor
 * 提供配合 datax存量同步的 增量消费binlog能力
 *
 */
@Component
@Slf4j
@DependsOn({"springContextHolder", "jobAdminConfig", "amqpAdmin"})
public class WenwoCanalClient {

    @Resource
    private Executor canalExecutor;

    @Resource
    private JobInfoMapper jobInfoMapper;

    @Autowired
    private CanalInstanceConfig canalInstanceConfig;

    /**
     * 工作线程
     */
    private final Map<String, CanalWorkThread> workThreads = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();


    @PostConstruct
    public void process() {
        initOrUpdate(true);
    }

    /**
     * 初始化或者更新
     */
    public void initOrUpdate(boolean isInit) {
        lock.lock();
        try {
            Map<String, InstanceInfo> instances = canalInstanceConfig.getInstances();
            if(CollectionUtils.isEmpty(instances)) {
                log.info("没有canal配置");
                return;
            }
            if (isInit) {
                // 初始化原有的canalJobs
                List<JobInfo> initCanalJobs = jobInfoMapper.findInitIncrInfo(ProjectConstant.INCREMENT_SYNC_TYPE.CANAL.val());
                if (CollectionUtil.isNotEmpty(initCanalJobs)) {
                    for (JobInfo initCanalJob : initCanalJobs) {
                        IncrementUtil.initCanal(initCanalJob, true);
                        IncrementUtil.addQueue(initCanalJob);
                    }
                }
            }

            // 首次初始化
            if (workThreads.isEmpty()) {
                for (String address : instances.keySet()) {
                    //启动canal消费线程
                    addTask(address);
                }
            } else {
                log.info("新老配置合并开始");
                Set<String> addressSet = instances.keySet();
                for (String address : addressSet) {
                    addTask(address);
                }
                // 移除废弃的 canal 监听任务
                List<String> removes = new ArrayList<>();
                for (String address : workThreads.keySet()) {
                    if (addressSet.contains(address)) {
                        continue;
                    }
                    removes.add(address);
                }
                if (!removes.isEmpty()) {
                    for (String remove : removes) {
                        removeTask(remove);
                    }
                }
            }
        } catch (Exception e) {
            log.error("initOrUpdate error", e);
        }finally {
            lock.unlock();
        }
    }

    /**
     * 添加任务
     * @param address
     */
    public void addTask(String address) {
        if(!workThreads.containsKey(address)) {
            //启动canal消费线程
            CanalWorkThread canalWorkThread = new CanalWorkThread(address, SpringContextHolder.getBean(MessageProducerService.class));
            canalExecutor.execute(canalWorkThread);
            workThreads.put(address, canalWorkThread);
            log.info("加入canal监听任务address:{}", address);
        }
    }

    /**
     * 移除任务
     * @param address
     */
    public void removeTask(String address) {
        if (workThreads.containsKey(address)) {
            workThreads.get(address).noRun();
            workThreads.get(address).interrupt();
            workThreads.remove(address);
            log.info("canal任务移除，address:{}", address);
        }
    }
}

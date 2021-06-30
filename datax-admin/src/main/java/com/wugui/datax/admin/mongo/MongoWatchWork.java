package com.wugui.datax.admin.mongo;

import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.util.IncrementUtil;
import com.wugui.datax.admin.entity.JobInfo;
import com.wugui.datax.admin.mapper.JobInfoMapper;
import com.wugui.datax.admin.mongo.ha.ConsistentHashSingleton;
import com.wugui.datax.admin.mongo.ha.ZkWatchNodeHA;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <pre>
 * 是如果要用于线上业务，还需要大量的测试，尤其是容错性与性能
 *
 * 监听指定mongo collection 的变化然后触发各种中间件操作
 * 如 rabbit,kafka消息, mysql es数据同步等等
 *
 * 当前bean需要依赖于高可用实现类 {@link ZkWatchNodeHA} 来启动
 *
 * @author ruanjl jiangling.ruan@pingcl.com
 * @since 2020/12/24 17:57
 */
@Slf4j
@Component
@DependsOn({"zkWatchNodeHA", "springContextHolder", "jobAdminConfig"})
public class MongoWatchWork {

    @Resource
    private Executor mongoWatchExecutor;

    @Resource
    private JobInfoMapper jobInfoMapper;

    /**
     * 工作线程
     */
    private final Map<String, Thread> workThreads = new ConcurrentHashMap<>();


    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 初始化工作任务, 或者动态更新任务
     * 这里会在三个时候被调用:
     * 1. 启动的时候初始化
     * 2. apollo配置变更
     * 3. zk 节点变更
     */
    @PostConstruct
    public void afterPropertiesSet() {
        lock.lock();
        try {
            initOrUpdate();
        } catch (Exception e) {
            log.error("initOrUpdate error", e);
        }finally {
            lock.unlock();
        }
    }

    /**
     * 初始化或者更新
     */
    private void initOrUpdate() {
        //Map<String, Set<Integer>> tableJobsMap = IncrementUtil.getTableJobsMap();
        /*Set<String> unionTables = IncrementUtil.getTableJobsMap().keySet();*/
        // 通过一致性hash找到本机器需要遍历的mongo集合
        /*Set<String> hashConsistentSet = ConsistentHashSingleton.instance().getSelfTasks(unionTables);
        if (hashConsistentSet.isEmpty()) {
            log.info("本机器没有分配到mongo watch任务");
            return;
        }*/
        // 首次初始化
        if (workThreads.isEmpty()) {
            // 初始化原有的MongoWatchJobs
            List<JobInfo> initMongoWatchJobs = jobInfoMapper.findInitIncrInfo(ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH.val());
            if (CollectionUtils.isEmpty(initMongoWatchJobs)) {
                log.info("initMongoWatchJobs is empty");
                return;
            }
            for (JobInfo initMongoWatchJob : initMongoWatchJobs) {
                IncrementUtil.initMongoWatch(initMongoWatchJob, true);
            }
            // 遍历对应的集合
           /* for (String unionTable : unionTables) {
                addTask(unionTable);
            }*/
        } else {
            log.info("新老配置合并开始");
            Set<String> unionTables = IncrementUtil.getTableJobsMap().keySet();
            // 添加新的 mongo collection 监听任务
            for (String unionTable : unionTables) {
                // 正在跑的线程任务
                addTask(unionTable);
            }
            // 移除废弃的 mongo collection 监听任务
            Set<String> hashConsistentSet = ConsistentHashSingleton.instance().getSelfTasks();
            Iterator<Map.Entry<String, Thread>> iter = workThreads.entrySet().iterator();
            List<String> removes = new ArrayList<>();
            Map.Entry<String, Thread> next;
            while (iter.hasNext()) {
                next = iter.next();
                String unionTable = next.getKey();
                if (hashConsistentSet.contains(unionTable)) {
                    continue;
                }
                removes.add(unionTable);
                // 这里执行之后, 被中断的线程会抛出异常, 代表该线程已经死亡, 任务移除
                next.getValue().interrupt();
                iter.remove();
            }
            log.info("老mongo collection 监听任务:{}被移除", removes);
        }
    }


    /**
     * 添加任务
     * @param unionTable
     */
    public void addTask(String unionTable) {
        if (!workThreads.containsKey(unionTable) && ConsistentHashSingleton.instance().addSelfTask(unionTable)) {
            String [] unionTableInfo = unionTable.split("_");
            MongoWatchWorkThread thread = new MongoWatchWorkThread(unionTableInfo[0], unionTableInfo[1], unionTableInfo[2]);
            mongoWatchExecutor.execute(thread);
            workThreads.put(unionTable, thread);
            log.info("初始化监听任务address:{}, database:{}, collection:{}", unionTableInfo[0], unionTableInfo[1], unionTableInfo[2]);
        }
    }

    /**
     * 移除任务
     * @param unionTable
     */
    public void removeTask(String unionTable) {
        // 这里执行之后, 被中断的线程会抛出异常, 代表该线程已经死亡, 任务移除
        if (workThreads.containsKey(unionTable)) {
            workThreads.get(unionTable).interrupt();
            workThreads.remove(unionTable);
            String[] unionTableInfo = unionTable.split("_");
            log.info("任务移除，address:{}, database:{}, collection:{}", unionTableInfo[0], unionTableInfo[1], unionTableInfo[2]);
        }
    }
}

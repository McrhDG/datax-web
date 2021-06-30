package com.wugui.datax.admin.mongo.ha;

import com.wugui.datax.admin.mongo.MongoWatchWork;
import com.wugui.datax.admin.util.SpringContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 需求：
 * 模拟实现HA 多点同时运行
 * <p>
 * 具体的功能：
 * 1. 启动时所有node都去parentNode下注册.
 * 2. node有变动的时候回调其余的节点触发任务变动.
 * 3. WatchNodeHA优先启动, 其余的类再启动. @Order(HIGHEST_PRECEDENCE)
 *
 * @author Doctor
 */
@Slf4j
@Component
public class ZkWatchNodeHA implements InitializingBean {

    private static final String SEPARATOR = "/";

    /**
     * zk连接信息
     */
    @Value("${zookeeper.address}")
    private String zkServers;

    /**
     * 会话超时时长会话建立成功最长的等待时间
     */
    @Value("${zookeeper.connection.timeout:15000}")
    private int timeout;

    /**
     * 初始化默认节点数
     */
    @Value("${init.start.node.count:3}")
    private int initStartNodeCount;

    private static final String WATCH_PARENT = "/wenwo/datax/mongo/watch";

    /**
     * 当前存活节点
     */
    private List<String> curNodes;

    /**
     * 当前节点名称, 默认是节点ip, 不能重复
     */
    @Value("${ip}")
    private String selfNode;

    /**
     * 等待当前节点删除次数
     */
    @Value("${temporary.node.retry:20}")
    private int nodeTry;

    /**
     * 是否已经启动, 用区分zookeeper的回调事件逻辑
     */
    private boolean init = false;

    private ZkClient zkClient;

    /**
     * 等待唤醒集群同时启动
     */
    private final Object start = new Object();

    /**
     * 注册当前节点，返回父节点下的所有子节点
     */
    @Override
    public void afterPropertiesSet() {
        zkClient = new ZkClient(zkServers, timeout);
        // 父节点持久节点
        if (!zkClient.exists(WATCH_PARENT)) {
            zkClient.createPersistent(WATCH_PARENT, true);
            log.info("父节点:{}创建成功", WATCH_PARENT);
        }
        // 创建机器的临时节点
        createSelfEphemeral();
        // 订阅父节点变化
        zkClient.subscribeChildChanges(WATCH_PARENT, (parentPath, currentChilds) -> applyChanges(currentChilds));
        // 获取父节点的子节点
        curNodes = zkClient.getChildren(WATCH_PARENT);
        // 等待启动完毕
        if (curNodes.size() < initStartNodeCount) {
            // 等待其他节点启动完毕通知唤醒当前
            synchronized (start) {
                try {
                    // 等待其他节点启动完毕通知唤醒当前节点直到超时
                    log.info("waiting for other nodes");
                    start.wait((long) initStartNodeCount * 10 * 1000);
                } catch (InterruptedException e) {
                    log.error("中断异常", e);
                }
            }
        }
        log.info("启动完毕:selfNode:{}, curNodes:{}", selfNode, curNodes);
        ConsistentHashSingleton.instance().setNodesCircle(curNodes, selfNode);
        init = true;
    }

    /**
     * 创建当前机器的临时节点
     */
    private void createSelfEphemeral() {
        String curPath = WATCH_PARENT + SEPARATOR + selfNode;
        int num = 0;
        while (zkClient.exists(curPath)) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            num++;
            if (num>nodeTry) {
                throw new RuntimeException("重试"+num+"次,节点已经存在, curPath:" + curPath + ",请检查selfNode值是否重复,或者等待当前临时节点删除完毕(20s左右)");
            }
        }
        log.info("重试{}次，等待当前临时节点删除", num);
        zkClient.createEphemeral(curPath);
    }

    /**
     * 父节点下面的节点数变更后的通知
     *
     * @param currentChilds 最新节点
     */
    private void applyChanges(List<String> currentChilds) {
        // currentChilds 为最新节点, curNodes 退化成老节点
        log.info("receive node change,selfNode:{} oldNodes:{}, currentNodes:{}",selfNode, curNodes, currentChilds);
        if (CollectionUtils.isEmpty(currentChilds)) {
            // 一般是本地调试的时候会走这个分支, 能收到监听信息, 说明本机是存活的, 将节点补上.
            createSelfEphemeral();
            return;
        } else if (currentChilds.size() == curNodes.size()) {
            return;
        }

        curNodes = currentChilds;
        if (init) {
            // 节点已经初始化, 节点新增或减少
            // 1. 触发hash一致性变化
            ConsistentHashSingleton.instance().setNodesCircle(currentChilds, selfNode);
            // 2. 触发对应的工作线程的变化
            // 由于 ZkWatchNodeHA 触发的比较早 所以采用这种方式获取MongoWatchWork bean
            SpringContextHolder.getBean(MongoWatchWork.class).afterPropertiesSet();
        } else {
            // 未初始化, 等到默认节点数达到后通知启动完毕
            if (curNodes.size() >= initStartNodeCount) {
                synchronized (start) {
                    start.notify();
                }
            }
        }
    }
}
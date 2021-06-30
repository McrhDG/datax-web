package com.wugui.datax.admin.mongo.ha;

import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * 一致性hash 单例工具类
 *
 * @author Doctor
 */
@Slf4j
public class ConsistentHashSingleton {

    private ConsistentHashSingleton(){}

    private static class ConsistentHashHolder{
        private static final ConsistentHashSingleton INSTANCE = new ConsistentHashSingleton();
    }

    public static ConsistentHashSingleton instance(){
        return ConsistentHashHolder.INSTANCE;
    }

    /**
     * 环形虚拟节点
     */
    private final SortedMap<Long, String> circle = new TreeMap<>();

    /**
     * 当前节点名称, 默认是节点ip, 不能重复
     */
    private String selfNode;

    /**
     * 每个机器节点关联的虚拟节点数量
     */
    private final static int HASH_REPLICA_COUNT = 100;

    /**
     *
     * @param nodes 全部节点
     * @param selfNode 当前节点 ip
     */
    public void setNodesCircle(List<String> nodes, String selfNode) {
        circle.clear();
        selfNodeCollection.clear();
        this.selfNode = selfNode;
        Collections.sort(nodes);
        for (String node : nodes) {
            add(node);
        }
    }

    /**
     * 增加真实机器节点
     *
     * @param node T
     */
    public void add(String node) {
        for (int i = 0; i < HASH_REPLICA_COUNT; i++) {
            circle.put(hash(node + i), node);
        }
    }

    public String get(String key) {
        if (circle.isEmpty()) {
            return null;
        }
        long hash = hash(key);
        // 沿环的顺时针找到一个虚拟节点
        if (!circle.containsKey(hash)) {
            SortedMap<Long, String> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

    /**
     * 当前节点的集合缓存
     */
    private Set<String> selfNodeCollection = new HashSet<>();

    /**
     * 用当前集合换取 一致性hash之后的集合
     *
     * @param collections 所有mongo collection
     * @return 一致性hash之后的集合(本机器需要消费的集合)
     */
    /*public Set<String> getSelfTasks(Set<String> collections) {
        selfNodeCollection.clear();
        for (String collection : collections) {
            String virtualNode = get(collection);
            // 根据一致性hash获取本机器的collection
            if (selfNode.equals(virtualNode)) {
                selfNodeCollection.add(collection);
            }
        }
        log.info("当前分配任务: selfNode:{}, hashConsistentSet:{}", selfNode, selfNodeCollection);
        return selfNodeCollection;
    }*/

    /**
     * 添加任务
     * @param collection
     */
    public boolean addSelfTask(String collection) {
        String virtualNode = get(collection);
        // 根据一致性hash获取本机器的collection
        if (selfNode.equals(virtualNode)) {
            selfNodeCollection.add(collection);
            return true;
        }
        return false;
    }

    /**
     * 获取任务集合
     * @return
     */
    public Set<String> getSelfTasks() {
        return selfNodeCollection;
    }


    /**
     * MurMurHash算法,性能高,碰撞率低
     *
     * @param key String
     * @return Long
     */
    public Long hash(String key) {
        ByteBuffer buf = ByteBuffer.wrap(key.getBytes());
        int seed = 0x1234ABCD;

        ByteOrder byteOrder = buf.order();
        buf.order(ByteOrder.LITTLE_ENDIAN);

        long m = 0xc6a4a7935bd1e995L;
        int r = 47;

        long h = seed ^ (buf.remaining() * m);

        long k;
        while (buf.remaining() >= 8) {
            k = buf.getLong();

            k *= m;
            k ^= k >>> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        if (buf.remaining() > 0) {
            ByteBuffer finish = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
            finish.put(buf).rewind();
            h ^= finish.getLong();
            h *= m;
        }

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        buf.order(byteOrder);
        return h;
    }

}
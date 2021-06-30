package com.wugui.datax.admin.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 数据同步线程父类
 * @author chen.ruihong
 * @date 2021-06-24
 */
@Slf4j
public class CanalWorkThread extends Thread {


    /**
     * 任务id集合
     */
    private List<Integer> jobIds = new CopyOnWriteArrayList<>();

    private final CanalConnector connector;

    private final String destination;

    /**
     * 构造函数
     * @param connector
     * @param destination
     */
    public CanalWorkThread(CanalConnector connector, String destination) {
        this.connector = connector;
        this.destination = destination;
    }


    @Override
    public void run() {

    }

    public String getDestination() {
        return destination;
    }
}
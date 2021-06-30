package com.wugui.datax.admin.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 线程池配置
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2021/6/24 10:45
 */
@Configuration
public class ThreadPoolConfig {

    /**
     * canal线程池核心线程数
     */
    @Value("${canal.thread-pool.core.size:5}")
    private int canalThreadPoolCore;

    /**
     * canal线程池最大线程数
     */
    @Value("${canal.thread-pool.max.size:20}")
    private int canalThreadPoolMax;

    /**
     * canal线程池等待队列长度
     */
    @Value("${canal.thread-pool.queue.length:"+Integer.MAX_VALUE+"}")
    private int canalThreadPoolQueue;

    /**
     * canal线程池线程活跃时间
     */
    @Value("${canal.thread-pool.keep-alive:60}")
    private int canalThreadPoolKeepAlive;



    /**
     * mongo watch线程池核心线程数
     */
    @Value("${mongo-watch.thread-pool.core.size:15}")
    private int mongoWatchThreadPoolCore;

    /**
     * mongo watch线程池最大线程数
     */
    @Value("${mongo-watch.thread-pool.max.size:100}")
    private int mongoWatchThreadPoolMax;

    /**
     * mongo watch线程池等待队列长度
     */
    @Value("${mongo-watch.thread-pool.queue.length:1}")
    private int mongoWatchThreadPoolQueue;

    /**
     * mongo watch线程池线程活跃时间
     */
    @Value("${mongo-watch.thread-pool.keep-alive:30}")
    private int mongoWatchThreadPoolKeepAlive;


    /**
     * canal运行线程池
     * @return
     */
    @Bean("canalExecutor")
    public Executor canalExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // 设置核心线程数
        executor.setCorePoolSize(canalThreadPoolCore);
        // 设置最大线程数
        executor.setMaxPoolSize(canalThreadPoolMax);
        //配置队列大小
        executor.setQueueCapacity(canalThreadPoolQueue);
        // 设置线程活跃时间（秒）
        executor.setKeepAliveSeconds(canalThreadPoolKeepAlive);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 等待所有任务结束后再关闭线程池
        executor.setWaitForTasksToCompleteOnShutdown(true);
        //执行初始化
        executor.initialize();
        return executor;
    }


    /**
     * mongo watch运行线程池
     * @return
     */
    @Bean("mongoWatchExecutor")
    public Executor mongoWatchExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // 设置核心线程数
        executor.setCorePoolSize(mongoWatchThreadPoolCore);
        // 设置最大线程数
        executor.setMaxPoolSize(mongoWatchThreadPoolMax);
        //配置队列大小
        executor.setQueueCapacity(mongoWatchThreadPoolQueue);
        // 设置线程活跃时间（秒）
        executor.setKeepAliveSeconds(mongoWatchThreadPoolKeepAlive);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 等待所有任务结束后再关闭线程池
        executor.setWaitForTasksToCompleteOnShutdown(true);
        //执行初始化
        executor.initialize();
        return executor;
    }
}

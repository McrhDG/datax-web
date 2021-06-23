package com.wugui.datax.admin.mq;

import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.util.SpringContextHolder;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

/**
 * RabbitMQ配置
 * @author Doctor
 * @date 2021/06/23
 */
@DependsOn("messageExchange")
@Configuration
public class RabbitConfig {


    /**
     * 端点同步队列
     * @return
     */
    @Bean
    public Queue endpointSyncQueue() {
        return new Queue(ProjectConstant.ENDPOINT_SYNC_QUEUE_PREFIX + JobAdminConfig.getAdminConfig().getIp(), true, false, true);
    }

    /**
     * 端点同步绑定
     * @return
     */
    @Bean
    public Binding bizErrorBinding() {
        return BindingBuilder.bind(endpointSyncQueue()).to(SpringContextHolder.getBean(TopicExchange.class)).with(ProjectConstant.ENDPOINT_SYNC_ROUTING_KEY);
    }

}

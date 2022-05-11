package com.wugui.datax.admin.mq;

import com.wugui.datax.admin.canal.CanalRabbitListener;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.util.SpringContextHolder;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.AbstractConnectionFactory;
import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
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

    @Bean
    public DirectMessageListenerContainer canalMessageListenerContainer(AbstractConnectionFactory connectionFactory){
        DirectMessageListenerContainer container = new DirectMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setMessageListener(new CanalRabbitListener());
        container.setPrefetchCount(1);
        container.setConsumersPerQueue(1);
        return container;
    }
}

package com.wugui.datax.admin.canal;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * canal配置
 * @author chen.ruihong
 * @date 2021-06-30
 */
@RefreshScope
@Component
@ConfigurationProperties(prefix = "canal")
@Data
public class CanalInstanceConfig {
	
	/** instance集合*/
	private Map<String, InstanceInfo> instances;
}

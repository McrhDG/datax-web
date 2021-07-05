package com.wugui.datax.admin.canal;

import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.ctrip.framework.apollo.spring.annotation.ApolloConfigChangeListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.context.scope.refresh.RefreshScope;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Set;

/**
 * <pre>
 * 触发apollo配置变更, 动态修改正在运行的任务
 * EnvironmentChangeEvent 或 RefreshScope
 *
 * @author ruanjl jiangling.ruan@pingcl.com
 * @since 2021/3/12 16:16
 */
@Slf4j
@Component
public class ApolloRefreshScope {

    @Autowired
    private RefreshScope refreshScope;

    @Resource
    private WenwoCanalClient wenwoCanalClient;

    @ApolloConfigChangeListener(value = {"canal.yml"})
    public void onChange(ConfigChangeEvent changeEvent) {
        Set<String> changedKeys = changeEvent.changedKeys();
        log.info("refreshing canal config changedKeys:{}", changedKeys);
        refreshScope.refresh("canalInstanceConfig");
        wenwoCanalClient.initOrUpdate(false);
    }

}

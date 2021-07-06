package com.wugui.datax.admin.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.wugui.datax.admin.util.SpringContextHolder;
import org.apache.commons.lang.StringUtils;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Instance工厂
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2021/6/30 16:46
 */
public class InstanceFactory {

    /**
     * 构造
     */
    private InstanceFactory() {
    }

    /**
     * canal连接
     * @return
     */
    public static CanalConnector getCanalConnector(String address) {
        InstanceInfo instanceInfo = getInstanceInfo(address);
        CanalConnector connector = null;
        if (StringUtils.isNotBlank(instanceInfo.getZkServers())) {
            connector = CanalConnectors.newClusterConnector(instanceInfo.getZkServers(), instanceInfo.getDestination(), instanceInfo.getCanalUsername(), instanceInfo.getCanalPassword());
        } else if (StringUtils.isNotBlank(instanceInfo.getCanalServerAddress())) {
            connector = CanalConnectors.newSingleConnector(new InetSocketAddress(instanceInfo.getCanalServerAddress(), instanceInfo.getCanalServerPort()),
                    instanceInfo.getDestination(), instanceInfo.getCanalUsername(), instanceInfo.getCanalPassword());
        }
        if (connector == null) {
            throw new RuntimeException("canal节点为空");
        }
        return connector;
    }

    /**
     * 获取instance信息
     * @param address
     * @return
     */
    private static InstanceInfo getInstanceInfo(String address) {
        CanalInstanceConfig canalInstanceConfig = SpringContextHolder.getBean(CanalInstanceConfig.class);
        Map<String, InstanceInfo> instances = canalInstanceConfig.getInstances();
        InstanceInfo instanceInfo = instances.get(address);
        if (instanceInfo==null) {
            throw new RuntimeException("canal节点为空");
        }
        return instanceInfo;
    }

    /**
     * 获取destination
     * @return
     */
    public static String getDestination(String address) {
        InstanceInfo instanceInfo = getInstanceInfo(address);
        return instanceInfo.getDestination();
    }

    /**
     * 获取canal注册地址
     * @param address
     * @return
     */
    public static String getCanalConnectionAddress(String address) {
        InstanceInfo instanceInfo = getInstanceInfo(address);
        return StringUtils.isNotBlank(instanceInfo.getZkServers())?instanceInfo.getZkServers():instanceInfo.getCanalServerAddress()+":"+instanceInfo.getCanalServerPort();
    }

    /**
     * 获取拉取频率
     * @param address
     * @return
     */
    public static Integer getPullInterval(String address) {
        InstanceInfo instanceInfo = getInstanceInfo(address);
        return instanceInfo.getPullInterval();
    }

    /**
     * 获取连接重试次数
     * @param address
     * @return
     */
    public static Integer getConnectionRetry(String address) {
        InstanceInfo instanceInfo = getInstanceInfo(address);
        return instanceInfo.getConnectionRetry();
    }
}

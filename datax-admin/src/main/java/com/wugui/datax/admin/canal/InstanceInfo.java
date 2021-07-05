package com.wugui.datax.admin.canal;

import lombok.Data;

/**
 * Instance信息
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2021/6/30 16:36
 */
@Data
public class InstanceInfo {

    /**
     * destination
     */
    private String destination;

    /**
     * zkServers
     */
    private String zkServers;

    /**
     * canalServerAddress
     */
    private String canalServerAddress;

    /**
     * canalServerPort
     */
    private Integer canalServerPort;

    /**
     * canalUsername
     */
    private String canalUsername;

    /**
     * canalPassword
     */
    private String canalPassword;

    /**
     * pullInterval
     */
    private Integer pullInterval = 1000;

    /**
     * connectionRetry
     */
    private Integer connectionRetry=5;

}

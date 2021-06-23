package com.wugui.datax.admin.constants;

/**
 * 项目常量
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2021/6/2 12:43
 */
public class ProjectConstant {

    /**
     * 私有构造
     */
    private ProjectConstant() {
    }

    /** mongodb_reader*/
    public static final String MONGODB_READER = "mongodbreader";

    /** mysql_reader*/
    public static final String MYSQL_READER = "mysqlreader";

    /**
     * 端点同步队列前缀
     */
    public static final String ENDPOINT_SYNC_QUEUE_PREFIX  = "datax.admin.endpoint.Sync.";

    /**
     * 端点同步路由
     */
    public static final String ENDPOINT_SYNC_ROUTING_KEY  = "endpoint.Sync";

    /** sourceIp*/
    public static final String SOURCE_IP = "sourceIp";
}

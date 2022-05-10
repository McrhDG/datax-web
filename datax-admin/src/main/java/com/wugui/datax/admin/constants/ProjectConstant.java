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

    /** mysql_writer*/
    public static final String MYSQL_WRITER = "mysqlwriter";

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

    /**
     * 增量同步方式
     */
    public enum INCREMENT_SYNC_TYPE {

        /** canal*/
        CANAL("canal"),
        /** mongo watch*/
        MONGO_WATCH("mongo_watch");

        private final String val;

        INCREMENT_SYNC_TYPE(String val) {
            this.val = val;
        }

        public String val(){
            return this.val;
        }
    }

    /**
     * 连接-数据库-表三级格式
     */
    public static final String URL_DATABASE_TABLE_FORMAT = "%s_%s_%s";

    /**
     * mongo 唯一表缓存key
     */
    public static final String REDIS_UNION_COLLECTION_KEY_FORMAT = "datax.admin.union.collection.%s_%s_%s";

    /**
     * canal增量同步任务名
     */
    public static final String CANAL_INCREMENT_TASK_NAME = "%s-%s-thread-%d";

    /**
     * mongo watch增量同步任务名
     */
    public static final String MONGO_WATCH_INCREMENT_TASK_NAME = "%s-%s-%s-thread-%d";

    /** type*/
    public static final String TYPE = "type";

    /** 默认mysql主键*/
    public static final String DEFAULT_MYSQL_KEY = "id";

    /**
     * 操作方式
     */
    public enum ACTION_TYPE {

        /** trigger*/
        TRIGGER("trigger"),
        /** update*/
        UPDATE("update"),
        /** remove*/
        REMOVE("remove");

        private final String val;

        ACTION_TYPE(String val) {
            this.val = val;
        }

        public String val(){
            return this.val;
        }
    }

    /** 锁自动过期时间 5分钟*/
    public final static Integer LOCK_TIMEOUT_300 = 300;

    /**
     * 增量等待锁
     */
    public static final String INCREMENT_WAIT_JOB_LOCK = "increment_wait_job_lock_%d";

    /**
     * canal队列
     */
    public static final String CANAL_JOB_QUEUE_FORMAT = "datax_admin_canal_job_queue_%d";

    /**
     * mongo队列
     */
    public static final String MONGO_JOB_QUEUE_FORMAT = "datax_admin_mongo_job_queue_%d";

    /**
     * 打印执行总数的条数
     */
    public final static Integer PRINT_EXECUTE_BATCH_SIZE = 10;
}

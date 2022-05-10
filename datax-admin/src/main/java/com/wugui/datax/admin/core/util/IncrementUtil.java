package com.wugui.datax.admin.core.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.Maps;
import com.mongodb.client.model.changestream.OperationType;
import com.wugui.datax.admin.canal.ColumnValue;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.entity.ConvertInfo;
import com.wugui.datax.admin.entity.IncrementSyncWaiting;
import com.wugui.datax.admin.entity.JobInfo;
import com.wugui.datax.admin.mongo.MongoWatchWork;
import com.wugui.datax.admin.util.MysqlUtil;
import com.wugui.datax.admin.util.SpringContextHolder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * 增量数据工具类
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2021/6/4 15:32
 */
public class IncrementUtil {

    private static final Logger log = LoggerFactory.getLogger(IncrementUtil.class);

    /**
     * 任务装换关系
     */
    private static final Map<Integer, ConvertInfo> CONVERT_INFO_MAP = new ConcurrentHashMap<>();

    /**
     * collection-任务关系
     */
    private static final Map<String, Set<Integer>> COLLECTION_JOBS_MAP = new ConcurrentHashMap<>();

    /**
     * 表-任务关系
     */
    private static final Map<String, Set<Integer>> TABLE_JOBS_MAP = new ConcurrentHashMap<>();

    /**
     * 正在全量同步的任务
     */
    private static final Set<Integer> RUNNING_JOBS = new CopyOnWriteArraySet<>();

    /**
     * 初始化增量同步器
     * @param jobInfo
     * @param isInit
     */
    public static void initIncrementData(JobInfo jobInfo, boolean isInit) {
        try {
            JSONObject content = IncrementUtil.getContent(jobInfo);
            if (content==null) {
                return;
            }
            // reader
            JSONObject reader = content.getJSONObject("reader");
            String name = reader.getString("name");
            if (ProjectConstant.MYSQL_READER.equalsIgnoreCase(name)) {
                initCanal(jobInfo, isInit);
            } else if (ProjectConstant.MONGODB_READER.equalsIgnoreCase(name)) {
                initMongoWatch(jobInfo, isInit);
            }
        } catch (Exception e) {
            log.error("initIncrementData error:", e);
        }
    }

    /**
     * @param jobInfo
     * @param isInit
     */
    public static void initCanal(JobInfo jobInfo, boolean isInit) {
        if (jobInfo.getIncrementType()>0) {
            log.info("jobId:{}, 存在自增设置，无需继续处理增量数据", jobInfo.getId());
            return;
        }

        JSONObject content = IncrementUtil.getContent(jobInfo);
        if (content==null) {
            return;
        }
        JSONObject reader = content.getJSONObject("reader");
        String name = reader.getString("name");
        if (!ProjectConstant.MYSQL_READER.equalsIgnoreCase(name)) {
            return;
        }
        JSONObject readerParam = reader.getJSONObject("parameter");
        JSONArray readerColumnArray = readerParam.getJSONArray("column");
        JSONArray readerConnections = readerParam.getJSONArray("connection");
        JSONObject readerConnectionJsonObj = readerConnections.getJSONObject(0);
        String querySql = readerConnectionJsonObj.getString("querySql");
        if (StringUtils.isNotBlank(querySql)) {
            log.info("jobId:{}, 存在自定义sql，无需继续处理增量数据", jobInfo.getId());
            return;
        }
        // writer
        JSONObject writer = content.getJSONObject("writer");
        String writerName = writer.getString("name");
        if (!ProjectConstant.MYSQL_WRITER.equalsIgnoreCase(writerName)) {
            return;
        }
        JSONObject writerParam = writer.getJSONObject("parameter");
        JSONArray writerColumnArray = writerParam.getJSONArray("column");
        //列对应的关系
        if(readerColumnArray.size() > writerColumnArray.size()) {
            log.info("jobId:{}, 字段无法对应，无法继续处理增量数据", jobInfo.getId());
            return;
        }

        String readerJdbcUrl = readerConnectionJsonObj.getJSONArray("jdbcUrl").getString(0);
        String readerTable = readerConnectionJsonObj.getJSONArray("table").getString(0);

        String readerDataBase = MysqlUtil.getMysqlDataBase(readerJdbcUrl);

        //表-任务
        String address =  MysqlUtil.getMysqlAddress(readerJdbcUrl);
        String tableUnion = String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, address, readerDataBase, readerTable);
        IncrementUtil.putTableJob(tableUnion, jobInfo.getId());

        // 首次初始化
        long initTimestamp;
        if (isInit) {
            initTimestamp = jobInfo.getIncrementSyncTime().getTime();
        } else {
            initTimestamp = System.currentTimeMillis();
            jobInfo.setIncrementSyncType(ProjectConstant.INCREMENT_SYNC_TYPE.CANAL.val());
            jobInfo.setIncrementSyncTime(new Date(initTimestamp));
            JobAdminConfig.getAdminConfig().getJobInfoMapper().update(jobInfo);
        }

        //装换关系
        Map<String, ConvertInfo.ToColumn> convertTableColumn = mysqlConvertTableColumn(readerColumnArray, writerColumnArray);
        putConvertInfo(jobInfo, writerParam, initTimestamp, convertTableColumn);
        log.info("jobId:{}, initCanal, init:{}", jobInfo.getId(), isInit);
    }

    /**
     * 新增队列
     * @param jobInfo
     */
    public static void addQueue(JobInfo jobInfo) {
        if (jobInfo==null || StringUtils.isBlank(jobInfo.getIncrementSyncType())) {
            return;
        }
        String queueName = String.format((ProjectConstant.INCREMENT_SYNC_TYPE.CANAL.val().equals(jobInfo.getIncrementSyncType())?ProjectConstant.CANAL_JOB_QUEUE_FORMAT:ProjectConstant.MONGO_JOB_QUEUE_FORMAT), jobInfo.getId());
        RabbitAdmin rabbitAdmin = SpringContextHolder.getBean(RabbitAdmin.class);
        QueueInformation queueInfo = rabbitAdmin.getQueueInfo(queueName);
        if (queueInfo==null) {
            Queue queue = new Queue(queueName, true, false, false);
            TopicExchange topicExchange = SpringContextHolder.getBean(TopicExchange.class);
            rabbitAdmin.declareExchange(topicExchange);
            rabbitAdmin.declareQueue(queue);
            Binding binding = BindingBuilder.bind(queue).to(topicExchange).with(String.valueOf(jobInfo.getId()));
            rabbitAdmin.declareBinding(binding);
        }
        DirectMessageListenerContainer container;
        if (ProjectConstant.INCREMENT_SYNC_TYPE.CANAL.val().equals(jobInfo.getIncrementSyncType())) {
            container = SpringContextHolder.getBean("canalMessageListenerContainer");
        } else {
            container = SpringContextHolder.getBean("mongoMessageListenerContainer");
        }
        container.addQueueNames(queueName);
    }

    /**
     *获取json内容
     * @param jobInfo
     * @return
     */
    public static JSONObject getContent(JobInfo jobInfo) {
        try {
            String encryptJobJson = jobInfo.getJobJson();
            JSONObject jsonObj = JSONObject.parseObject(encryptJobJson);
            JSONObject jobJson = jsonObj.getJSONObject("job");
            JSONArray contents = jobJson.getJSONArray("content");
            return contents.getJSONObject(0);
        } catch (Exception e) {
            log.error("getContent, error:", e);
        }
        return null;
    }


    /**
     * @param jobInfo
     */
    public static void initMongoWatch(JobInfo jobInfo, boolean isInit) {
        if (jobInfo.getIncrementType()>0) {
            log.info("jobId:{}, 存在自增设置，无需继续处理增量数据", jobInfo.getId());
            return;
        }

        JSONObject content = IncrementUtil.getContent(jobInfo);
        if (content==null) {
            return;
        }
        JSONObject reader = content.getJSONObject("reader");
        String readerName = reader.getString("name");
        if (!ProjectConstant.MONGODB_READER.equalsIgnoreCase(readerName)) {
            return;
        }
        // writer
        JSONObject writer = content.getJSONObject("writer");
        String writerName = writer.getString("name");
        if (!ProjectConstant.MYSQL_WRITER.equalsIgnoreCase(writerName)) {
            return;
        }
        JSONObject readerParam = reader.getJSONObject("parameter");
        JSONArray readerColumnArray = readerParam.getJSONArray("column");

        JSONObject writerParam = writer.getJSONObject("parameter");
        JSONArray writerColumnArray = writerParam.getJSONArray("column");
        if(readerColumnArray.size() > writerColumnArray.size()) {
            log.info("jobId:{}, 字段无法对应，无法继续处理增量数据", jobInfo.getId());
            return;
        }
        //表-任务
        String tableUnion = getMongoUnion(readerParam);
        IncrementUtil.putCollectionJob(tableUnion, jobInfo.getId());


        // 首次初始化
        long initTimestamp;
        if (isInit) {
            initTimestamp = jobInfo.getIncrementSyncTime().getTime();
        } else {
            initTimestamp = System.currentTimeMillis();
            jobInfo.setIncrementSyncType(ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH.val());
            jobInfo.setIncrementSyncTime(new Date(initTimestamp));
            JobAdminConfig.getAdminConfig().getJobInfoMapper().update(jobInfo);
        }

        //装换关系
        Map<String, ConvertInfo.ToColumn> convertTableColumn = mongoConvertTableColumn(readerColumnArray, writerColumnArray);
        putConvertInfo(jobInfo, writerParam, initTimestamp, convertTableColumn);

        MongoWatchWork mongoWatchWork = SpringContextHolder.getBean(MongoWatchWork.class);
        mongoWatchWork.addTask(tableUnion);
        log.info("jobId:{}, initMongoWatch, init:{}", jobInfo.getId(), isInit);
    }

    /**
     * 获取mongo唯一标识
     * @param readerParam
     * @return
     */
    private static String getMongoUnion(JSONObject readerParam) {
        String readerDataBase = readerParam.getString("dbName");
        String collection = readerParam.getString("collectionName");
        Set<String> addressSet = readerParam.getObject("address", new TypeReference<Set<String>>(){});
        String address = StringUtils.join(addressSet, ',');
        return String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, address, readerDataBase, collection);
    }

    /**
     * 装换关系設置
     * @param jobInfo
     * @param writerParam
     * @param initTimestamp
     * @param convertTableColumn
     */
    private static void putConvertInfo(JobInfo jobInfo, JSONObject writerParam, long initTimestamp, Map<String, ConvertInfo.ToColumn> convertTableColumn) {
        JSONArray writerConnections = writerParam.getJSONArray("connection");
        JSONObject writerConnectionJsonObj = writerConnections.getJSONObject(0);
        String writerJdbcUrl = writerConnectionJsonObj.getString("jdbcUrl");
        String writerUrl = MysqlUtil.getMysqlUrl(writerJdbcUrl);
        String writeTable = writerConnectionJsonObj.getJSONArray("table").getString(0);
        ConvertInfo convertInfo = new ConvertInfo(jobInfo.getId(), writeTable, convertTableColumn, writerUrl, initTimestamp);
        IncrementUtil.putConvertInfo(jobInfo.getId(), convertInfo);
    }


    /**
     * 加入装换信息
     * @param jobId
     * @param convertInfo
     */
    public static void putConvertInfo(Integer jobId, ConvertInfo convertInfo) {
        CONVERT_INFO_MAP.put(jobId, convertInfo);
    }

    /**
     * 获取转换信息
     * @param jobId
     * @return
     */
    public static ConvertInfo getConvertInfo(Integer jobId) {
        return CONVERT_INFO_MAP.get(jobId);
    }

    /**
     * 加入表任务关系
     * @param tableUnion
     * @param jobId
     */
    public static void putTableJob(String tableUnion, Integer jobId) {
        Set<Integer> jobIds = TABLE_JOBS_MAP.get(tableUnion);
        if (CollectionUtils.isEmpty(jobIds)) {
            jobIds = new HashSet<>();
        }
        jobIds.add(jobId);
        TABLE_JOBS_MAP.put(tableUnion, jobIds);
    }

    /**
     * 获取表任务关系
     * @return
     */
    public static Map<String, Set<Integer>> getTableJobsMap() {
        return TABLE_JOBS_MAP;
    }

    /**
     * 加入表任务关系
     * @param tableUnion
     * @param jobId
     */
    public static void putCollectionJob(String tableUnion, Integer jobId) {
        Set<Integer> jobIds = COLLECTION_JOBS_MAP.get(tableUnion);
        if (CollectionUtils.isEmpty(jobIds)) {
            jobIds = new HashSet<>();
        }
        jobIds.add(jobId);
        COLLECTION_JOBS_MAP.put(tableUnion, jobIds);
    }

    /**
     * 获取表任务关系
     * @return
     */
    public static Map<String, Set<Integer>> getCollectionJobsMap() {
        return COLLECTION_JOBS_MAP;
    }

    /**
     * mysql字段转换关系
     * @param readerColumn
     * @param writerColumn
     */
    public static Map<String, ConvertInfo.ToColumn>  mysqlConvertTableColumn(JSONArray readerColumn, JSONArray writerColumn) {
        Map<String, ConvertInfo.ToColumn> map = Maps.newHashMapWithExpectedSize(readerColumn.size());
        for (int i = 0; i < readerColumn.size(); i++) {
            String key = readerColumn.getString(i);
            String value = writerColumn.getString(i);
            if(StringUtils.isNotBlank(value)) {
                map.put(key.replace("`", ""), new ConvertInfo.ToColumn(value.replace("`", "")));
            }
        }
        return map;
    }

    /**
     * mongo 字段转换关系
     * @param readerColumn
     * @param writerColumn
     * @return
     */
    public static Map<String, ConvertInfo.ToColumn> mongoConvertTableColumn(JSONArray readerColumn, JSONArray writerColumn) {
        Map<String, ConvertInfo.ToColumn> map = Maps.newHashMapWithExpectedSize(readerColumn.size());
        for (int i = 0; i < readerColumn.size(); i++) {
            JSONObject jObj = readerColumn.getJSONObject(i);
            String key = jObj.getString("name");
            String type = jObj.getString("type");
            String splitter = jObj.getString("splitter");
            String value = writerColumn.getString(i);
            if(StringUtils.isNotBlank(value)) {
                map.put(key, new ConvertInfo.ToColumn(value.replace("`", ""), type, splitter));
            }
        }
        return map;
    }

    /**
     * 进入运行中任务
     * @param jobId
     */
    public static void addRunningJob(Integer jobId) {
        RUNNING_JOBS.add(jobId);
    }

    /**
     * 是否运行中任务
     * @param jobId
     * @return
     */
    public static boolean isRunningJob(Integer jobId) {
        return RUNNING_JOBS.contains(jobId);
    }

    /**
     * 移除正在运行任务（执行完毕了）
     * @param jobId
     */
    public static void removeRunningJob(Integer jobId) {
        RUNNING_JOBS.remove(jobId);
    }

    /**
     * 移除任务
     * @param jobInfo
     */
    public static void removeTask(JobInfo jobInfo) {
        try{
            JSONObject content = IncrementUtil.getContent(jobInfo);
            if (content==null) {
                return;
            }
            JSONObject reader = content.getJSONObject("reader");
            String readerName = reader.getString("name");
            if (ProjectConstant.MONGODB_READER.equalsIgnoreCase(readerName)) {
                JSONObject readerParam = reader.getJSONObject("parameter");
                String tableUnion = getMongoUnion(readerParam);
                if (COLLECTION_JOBS_MAP.containsKey(tableUnion)) {
                    Set<Integer> jobIds = COLLECTION_JOBS_MAP.get(tableUnion);
                    if (!jobIds.isEmpty()) {
                        jobIds.remove(jobInfo.getId());
                    }

                    if (jobIds.isEmpty()) {
                        COLLECTION_JOBS_MAP.remove(tableUnion);
                        MongoWatchWork mongoWatchWork = SpringContextHolder.getBean(MongoWatchWork.class);
                        mongoWatchWork.removeTask(tableUnion);
                    }
                }
            } else if(ProjectConstant.MYSQL_READER.equalsIgnoreCase(readerName)) {
                JSONObject readerParam = reader.getJSONObject("parameter");
                JSONArray readerConnections = readerParam.getJSONArray("connection");
                JSONObject readerConnectionJsonObj = (JSONObject) readerConnections.get(0);
                String readerJdbcUrl = readerConnectionJsonObj.getJSONArray("jdbcUrl").getString(0);
                String readerAddress = MysqlUtil.getMysqlAddress(readerJdbcUrl);
                String readerDataBase = MysqlUtil.getMysqlDataBase(readerJdbcUrl);
                String readerTable = readerConnectionJsonObj.getJSONArray("table").getString(0);
                String tableUnion = String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, readerAddress, readerDataBase, readerTable);
                if (TABLE_JOBS_MAP.containsKey(tableUnion)) {
                    Set<Integer> jobIds = TABLE_JOBS_MAP.get(tableUnion);
                    if (!jobIds.isEmpty()) {
                        jobIds.remove(jobInfo.getId());
                    }

                    if (jobIds.isEmpty()) {
                        TABLE_JOBS_MAP.remove(tableUnion);
                    }
                }
            }
            CONVERT_INFO_MAP.remove(jobInfo.getId());

            removeMessageListenerContainer(jobInfo);

            //jobInfo.setIncrementSyncType(null);
            jobInfo.setIncrementSyncTime(null);
            log.info("jobId:{}, removeTask", jobInfo.getId());
        } catch (Exception e) {
            log.error("removeTask error:", e);
        }
    }

    /**
     * 移除消息监听
     * @param jobInfo
     */
    private static void removeMessageListenerContainer(JobInfo jobInfo) {
        //删除队列
        if (jobInfo.getIncrementSyncType()!=null) {
            DirectMessageListenerContainer container;
            String queueName;
            if (ProjectConstant.INCREMENT_SYNC_TYPE.CANAL.val().equals(jobInfo.getIncrementSyncType())) {
                queueName = String.format(ProjectConstant.CANAL_JOB_QUEUE_FORMAT, jobInfo.getId());
                container = SpringContextHolder.getBean("canalMessageListenerContainer");
            } else {
                queueName = String.format(ProjectConstant.MONGO_JOB_QUEUE_FORMAT, jobInfo.getId());
                container = SpringContextHolder.getBean("mongoMessageListenerContainer");
            }
            container.removeQueueNames(queueName);
            RabbitAdmin rabbitAdmin = SpringContextHolder.getBean(RabbitAdmin.class);
            rabbitAdmin.deleteQueue(queueName);
        }
    }

    /**
     * 是下一个循环
     * @param jobId
     * @param operationType
     * @param syncType
     * @param executeTime
     * @param content
     * @param condition
     * @param idValue
     * @return
     */
    public static ConvertInfo isContinue(int jobId, String operationType, ProjectConstant.INCREMENT_SYNC_TYPE syncType, long executeTime, Object content, Object condition, String idValue) {
        ConvertInfo convertInfo = IncrementUtil.getConvertInfo(jobId);
        if (convertInfo == null) {
            log.error("jobId:{}, 不存在读写装换关系", jobId);
            return null;
        }
        if (executeTime < convertInfo.getInitTimestamp()) {
            return null;
        }
        if (IncrementUtil.isRunningJob(jobId)) {
            saveWaiting(jobId, operationType, syncType, content, condition, idValue);
            return null;
        }
        return convertInfo;
    }

    /**
     * 保存等待任务
     * @param jobId
     * @param operationType
     * @param syncType
     * @param content
     * @param condition
     * @param idValue
     */
    public static void saveWaiting(int jobId, String operationType, ProjectConstant.INCREMENT_SYNC_TYPE syncType, Object content, Object condition, String idValue) {
        if (OperationType.INSERT.name().equals(operationType)) {
            //插入语句清除更新语句、之前的插入语句
            JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().deleteByOperation(jobId, idValue, OperationType.UPDATE.name());
            JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().deleteByOperation(jobId, idValue, OperationType.INSERT.name());
            //保存插入语句
            IncrementSyncWaiting incrementSyncWaiting = IncrementSyncWaiting.builder()
                    .jobId(jobId).type(syncType.val())
                    .operationType(OperationType.INSERT.name())
                    .content(JSON.toJSONString(content, SerializerFeature.WriteDateUseDateFormat))
                    .idValue(idValue).build();
            JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().save(incrementSyncWaiting);
        } else if (OperationType.UPDATE.name().equals(operationType)) {
            //更新语句保留一条就够了
            IncrementSyncWaiting incrementSyncWaiting = JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().loadUpdate(jobId);
            if(incrementSyncWaiting != null) {
                //更新更新字段
                String updateContent = incrementSyncWaiting.getContent();
                if (ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH==syncType) {
                    JSONObject jsonObject = JSON.parseObject(updateContent);
                    Map<String, Object> mongoData = (Map<String, Object>) content;
                    mongoData.putAll(jsonObject);
                    updateContent = JSON.toJSONString(mongoData, SerializerFeature.WriteDateUseDateFormat);
                } else if (ProjectConstant.INCREMENT_SYNC_TYPE.CANAL==syncType) {
                    Map<String, ColumnValue> updateMap = JSON.parseObject(updateContent, new TypeReference<Map<String, ColumnValue>>(){});
                    Map<String, ColumnValue> mysqlData = (Map<String, ColumnValue>) content;
                    updateMap.putAll(mysqlData);
                    updateContent = JSON.toJSONString(updateMap, SerializerFeature.WriteDateUseDateFormat);
                }
                JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().updateContent(incrementSyncWaiting.getId(), updateContent);
            } else {
                //保存更新语句
                IncrementSyncWaiting.IncrementSyncWaitingBuilder builder =  IncrementSyncWaiting.builder();
                builder.jobId(jobId).type(syncType.val())
                        .operationType(OperationType.UPDATE.name())
                        .content(JSON.toJSONString(content, SerializerFeature.WriteDateUseDateFormat))
                        .idValue(idValue);
                if (ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH==syncType) {
                    builder.condition((String) condition);
                } else if (ProjectConstant.INCREMENT_SYNC_TYPE.CANAL==syncType) {
                    builder.condition(JSON.toJSONString(condition));
                }
                incrementSyncWaiting = builder.build();
                JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().save(incrementSyncWaiting);
            }
        } else if (OperationType.DELETE.name().equals(operationType)) {
            //删除语句清除所有
            JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().deleteByJobIdAndIdValue(jobId, idValue);
            //保存删除语句
            IncrementSyncWaiting.IncrementSyncWaitingBuilder builder =  IncrementSyncWaiting.builder();
            builder.jobId(jobId).type(syncType.val())
                    .operationType(OperationType.DELETE.name())
                    .idValue(idValue);
            if (ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH==syncType) {
                builder.condition((String) condition);
            } else if (ProjectConstant.INCREMENT_SYNC_TYPE.CANAL==syncType) {
                builder.condition(JSON.toJSONString(condition));
            }
            IncrementSyncWaiting incrementSyncWaiting = builder.build();
            JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().save(incrementSyncWaiting);
        }
    }
}

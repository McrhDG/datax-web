package com.wugui.datax.admin.mongo;

import cn.hutool.core.io.watch.WatchException;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.core.util.IncrementUtil;
import com.wugui.datax.admin.entity.ConvertInfo;
import com.wugui.datax.admin.entity.IncrementSyncWaiting;
import com.wugui.datax.admin.util.MongoUtil;
import com.wugui.datax.admin.util.MysqlUtil;
import com.wugui.datax.admin.util.SpringContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据同步线程父类
 * @author chen.ruihong
 * @date 2021-06-24
 */
@Slf4j
public class MongoWatchWorkThread extends Thread {

    private static final AtomicInteger THREAD_NUMBER = new AtomicInteger(1);

    private final String connectUrl;

    private final String database;

    private final String collection;

    private final RedisTemplate<String, String> redisTemplate = SpringContextHolder.getBean("redisTemplate");

    /**
     * 构造函数
     * @param connectUrl
     * @param database
     * @param collection
     */
    public MongoWatchWorkThread(String connectUrl, String database, String collection) {
        this.connectUrl = connectUrl;
        this.database = database;
        this.collection = collection;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(String.format(ProjectConstant.INCREMENT_TASK_NAME, "changeStreamWorker", database, collection, THREAD_NUMBER.getAndIncrement()));
        int retry = 0;
        long sleepTime = 1000L;
        Thread currentThread = Thread.currentThread();
        int connectionTry = JobAdminConfig.getAdminConfig().getMongoConnectionTry();
        String redisUnionTable = String.format(ProjectConstant.REDIS_UNION_COLLECTION__KEY_FORMAT, connectUrl, database, collection);
        MongoClient mongoClient;
        while (true) {
            try {
                String tableUnion = String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, connectUrl, database, collection);
                Set<Integer> jobIds  = IncrementUtil.getTableJobsMap().get(tableUnion);
                if (CollectionUtils.isEmpty(jobIds)) {
                    log.error("jobIds is empty, finish the task");
                    return;
                }
                Integer [] jobIdArray = jobIds.toArray(new Integer[0]);
                mongoClient = MongoUtil.getMongoClient(connectUrl, jobIdArray[0]);
                if (mongoClient==null) {
                    log.error("url:{}, mongoClient is null", connectUrl);
                    return;
                }
                // 2. 根据 token 获取流事件
                ChangeStreamIterable<Document> stream;
                String redisToken = redisTemplate.opsForValue().get(redisUnionTable);
                MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
                MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
                if (StringUtils.isNotBlank(redisToken)) {
                    // redis 的优先级其次
                    BsonDocument resumeDoc = new BsonDocument().append("_data", new BsonString(redisToken));
                    stream = mongoCollection.watch().resumeAfter(resumeDoc).fullDocument(FullDocument.UPDATE_LOOKUP);
                } else {
                    stream = mongoCollection.watch().fullDocument(FullDocument.UPDATE_LOOKUP);
                }
                String resumeToken;
                // 3. 消费mongo流事件. 如果没有数据流事件，这里会持续阻塞，直到下一个流事件到来
                for (ChangeStreamDocument<Document> streamDoc : stream) {
                    OperationType operationType = streamDoc.getOperationType();
                    Document fullDocument = streamDoc.getFullDocument();
                    BsonDocument documentKey = streamDoc.getDocumentKey();
                    BsonTimestamp clusterTime = streamDoc.getClusterTime();
                    resumeToken = streamDoc.getResumeToken().getString("_data").getValue();
                    log.debug("database:[{}],collection:[{}], resumeToken:{}, id:{}", database, collection, resumeToken, documentKey);
                    redisTemplate.opsForValue().set(redisUnionTable, resumeToken);
                    try {
                        switch (operationType) {
                            case INSERT:
                                insert(fullDocument, documentKey, clusterTime);
                                break;
                            case UPDATE:
                                UpdateDescription updateDescription = streamDoc.getUpdateDescription();
                                update(updateDescription, documentKey, clusterTime);
                                break;
                            case REPLACE:
                                replace(fullDocument, documentKey, clusterTime);
                                break;
                            case DELETE:
                                delete(documentKey, clusterTime);
                                break;
                            default:
                                log.info("operation:{} not support, database:{}, collection:{}", operationType, database, collection);
                        }
                    } catch (Exception e) {
                        log.error("collection:[{}], resumeToken:{}, id:{}", collection, resumeToken, documentKey, e);
                        break;
                    }
                }
                //线程中断给予退出
                if (currentThread.isInterrupted()) {
                    log.info("thread is Interrupted, exit the thread");
                    return;
                }
            } catch (Throwable e) {
                log.error("process error!", e);
                try {
                    if (retry < connectionTry) {
                        retry ++;
                        sleepTime = sleepTime * retry;
                    }
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e1) {
                    log.error("InterruptedException", e1);
                }
                //线程中断给予退出
                if (currentThread.isInterrupted()) {
                    log.info("thread is Interrupted, exit the thread");
                    return;
                }
            }
        }
    }

    /**
     * 插入
     * @param fullDocument
     * @param documentKey
     * @param clusterTime
     */
    private void insert(Document fullDocument, BsonDocument documentKey, BsonTimestamp clusterTime) {
        Map<String, Object> mongoData = MongoUtil.toParseMap(fullDocument, documentKey);
        String id = MongoUtil.getDocumentKeyId(documentKey);
        String tableUnion = String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, connectUrl, database, collection);
        Set<Integer> jobIds  = IncrementUtil.getTableJobsMap().get(tableUnion);
        if (CollectionUtils.isEmpty(jobIds)) {
            return;
        }
        for (Integer jobId : jobIds) {
            if (IncrementUtil.isRunningJob(jobId)) {
                //插入语句清除更新语句、之前的插入语句
                JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().deleteByOperation(jobId, id, OperationType.UPDATE.name());
                JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().deleteByOperation(jobId, id, OperationType.INSERT.name());
                //保存插入语句
                IncrementSyncWaiting incrementSyncWaiting = IncrementSyncWaiting.builder()
                        .jobId(jobId).type(ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH.val())
                        .operationType(OperationType.INSERT.name())
                        .content(JSON.toJSONString(mongoData, SerializerFeature.WriteDateUseDateFormat))
                        .idValue(id).build();
                JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().save(incrementSyncWaiting);
                continue;
            }
            ConvertInfo convertInfo = IncrementUtil.getConvertInfo(jobId);
            if (convertInfo == null) {
                log.error("jobId:{}, 不存在读写装换关系", jobId);
                continue;
            }
            if (clusterTime.getTime()*1000L<convertInfo.getInitTimestamp()) {
                continue;
            }
            insert(convertInfo, mongoData);
        }
    }

    /**
     * 更新
     * @param updateDescription
     * @param documentKey
     * @param clusterTime
     */
    private void update(UpdateDescription updateDescription, BsonDocument documentKey, BsonTimestamp clusterTime) {
        if (updateDescription!=null && !CollectionUtils.isEmpty(updateDescription.getUpdatedFields())) {
            Map<String, Object> mongoData = MongoUtil.toParseMap(updateDescription.getUpdatedFields());
            update(mongoData, documentKey, clusterTime);
        }
    }

    /**
     * 更新
     * @param fullDocument
     * @param documentKey
     * @param clusterTime
     */
    private void replace(Document fullDocument, BsonDocument documentKey, BsonTimestamp clusterTime) {
        Map<String, Object> mongoData = MongoUtil.toParseMap(fullDocument, documentKey);
        update(mongoData, documentKey, clusterTime);
    }

    /**
     * 更新前置
     * @param mongoData
     * @param documentKey
     * @param clusterTime
     */
    private void update(Map<String, Object> mongoData, BsonDocument documentKey, BsonTimestamp clusterTime) {
        String id = MongoUtil.getDocumentKeyId(documentKey);
        String tableUnion = String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, connectUrl, database, collection);
        Set<Integer> jobIds  = IncrementUtil.getTableJobsMap().get(tableUnion);
        if (CollectionUtils.isEmpty(jobIds)) {
            return;
        }
        for (Integer jobId : jobIds) {
            if (IncrementUtil.isRunningJob(jobId)) {
                //更新语句保留一条就够了
                IncrementSyncWaiting incrementSyncWaiting = JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().loadUpdate(jobId);
                if(incrementSyncWaiting !=null) {
                    //更新更新字段
                    String content = incrementSyncWaiting.getContent();
                    JSONObject jsonObject = JSON.parseObject(content);
                    mongoData.putAll(jsonObject);
                    content = JSON.toJSONString(mongoData, SerializerFeature.WriteDateUseDateFormat);
                    JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().updateContent(incrementSyncWaiting.getId(), content);
                } else {
                    //保存更新语句
                    incrementSyncWaiting = IncrementSyncWaiting.builder()
                            .jobId(jobId).type(ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH.val())
                            .operationType(OperationType.UPDATE.name())
                            .content(JSON.toJSONString(mongoData, SerializerFeature.WriteDateUseDateFormat))
                            .condition(id)
                            .idValue(id).build();
                    JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().save(incrementSyncWaiting);
                }
                continue;
            }
            ConvertInfo convertInfo = IncrementUtil.getConvertInfo(jobId);
            if (convertInfo == null) {
                log.error("jobId:{}, 不存在读写装换关系", jobId);
                continue;
            }
            if (clusterTime.getTime()*1000L<convertInfo.getInitTimestamp()) {
                continue;
            }
            update(convertInfo, mongoData, id);
        }
    }

    /**
     * 删除
     * @param documentKey
     * @param clusterTime
     */
    private void delete(BsonDocument documentKey, BsonTimestamp clusterTime) {
        String id = MongoUtil.getDocumentKeyId(documentKey);
        String tableUnion = String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, connectUrl, database, collection);
        Set<Integer> jobIds  = IncrementUtil.getTableJobsMap().get(tableUnion);
        if (CollectionUtils.isEmpty(jobIds)) {
            return;
        }
        for (Integer jobId : jobIds) {
            if (IncrementUtil.isRunningJob(jobId)) {
                //删除语句清除所有
                JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().deleteByJobIdAndIdValue(jobId, id);
                //保存删除语句
                IncrementSyncWaiting incrementSyncWaiting = IncrementSyncWaiting.builder()
                        .jobId(jobId).type(ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH.val())
                        .operationType(OperationType.DELETE.name())
                        .condition(id)
                        .idValue(id).build();
                JobAdminConfig.getAdminConfig().getIncrementSyncWaitingMapper().save(incrementSyncWaiting);
                continue;
            }
            ConvertInfo convertInfo = IncrementUtil.getConvertInfo(jobId);
            if (convertInfo == null) {
                log.error("jobId:{}, 不存在读写装换关系", jobId);
                continue;
            }
            if (clusterTime.getTime()*1000L<convertInfo.getInitTimestamp()) {
                continue;
            }
            delete(convertInfo, id);
        }
    }

    /**
     * 插入
     * @param convertInfo
     * @param mongoData
     */
    public static void insert(ConvertInfo convertInfo, Map<String, Object> mongoData) {
        // 数据源中的各个表
        Map<String, String> relation = convertInfo.getTableColumns();
        Map<String, Object> mysqlData = columnConvert(mongoData, relation);
        // 每个表 执行sql
        String insertSql;
        insertSql = convertInfo.getInsertSql();
        if (StringUtils.isBlank(insertSql)) {
            insertSql = doGetInsertSql(convertInfo.getTableName(), mysqlData);
            convertInfo.setInsertSql(insertSql);
        }

        DataSource dataSource = MysqlUtil.getDataSource(convertInfo.getWriteUrl(), convertInfo.getJobId());
        if (dataSource==null) {
            log.error("jobId:{}, 无法找到可用数据源", convertInfo.getJobId());
            return;
        }
        int update = 0;
        int index = 0;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
            List<Object> valueParam = new ArrayList<>();
            setValues(index, preparedStatement, mysqlData, valueParam);
            log.info("insertSql:{}, value:{}", insertSql, valueParam);
            update = preparedStatement.executeUpdate();
        } catch (Exception e) {
            log.error("columns.size:{} index:{}, error:", mysqlData.size(), index, e);
        } finally {
            try {
                assert connection != null;
                // 回收,并非关闭
                connection.close();
            } catch (SQLException e) {
                log.error("close Exception", e);
            }
        }
        log.info("statement.executeInsert result:{}", update);
    }

    /**
     * 列转换: 从mongo 的列 转换为 mysql的列
     *
     * @param mongoData mongo的数据map
     * @param relation 列转换关系
     * @return 转换后的 mysql数据
     */
    private static Map<String, Object> columnConvert(Map<String, Object> mongoData, Map<String, String> relation) {
        // 这里使用 TreeMap
        TreeMap<String, Object> mysqlData = new TreeMap<>();
        for (Map.Entry<String, String> entry : relation.entrySet()) {
            String fromColumn = entry.getKey();
            String toColumn = entry.getValue();
            if (mongoData.containsKey(fromColumn)) {
                mysqlData.put(toColumn, mongoData.get(fromColumn));
            }
        }
        return mysqlData;
    }

    /**
     *
     * @param tableName
     * @param data
     * @return
     */
    private static String doGetInsertSql(String tableName, Map<String, Object> data) {
        String insertSql;
        String column;
        StringBuilder insertBuilder = new StringBuilder("INSERT IGNORE `");
        insertBuilder.append(tableName);
        insertBuilder.append("` (");
        StringBuilder placeholders = new StringBuilder("VALUES(");
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            column = entry.getKey();
            insertBuilder.append("`").append(column).append("`").append(",");
            placeholders.append("?").append(",");
        }
        insertBuilder.delete(insertBuilder.length() - 1, insertBuilder.length()).append(")");
        placeholders.delete(placeholders.length() - 1, placeholders.length()).append(")");
        insertSql = insertBuilder.append(placeholders).toString();
        return insertSql;
    }

    /**
     * 设置值
     * @param index
     * @param preparedStatement
     * @param mysqlData
     * @param valueParam
     * @throws SQLException
     */
    private static int setValues(int index, PreparedStatement preparedStatement, Map<String, Object> mysqlData, List<Object> valueParam) throws SQLException {
        Object value;
        for (Map.Entry<String, Object> entry : mysqlData.entrySet()) {
            index ++;
            value = entry.getValue();
            valueParam.add(value);
            if (value==null) {
                preparedStatement.setNull(index, Types.OTHER);
                continue;
            }
            preparedStatement.setObject(index, value);
        }
        return index;
    }

    /**
     * 更新
     * @param convertInfo
     * @param updateData
     * @param idValue
     */
    public static void update(ConvertInfo convertInfo, Map<String, Object> updateData, String idValue) {
        // 数据源中的各个表
        Map<String, String> relation = convertInfo.getTableColumns();

        Map<String, Object> mysqlData = columnConvert(updateData, relation);
        // 执行sql
        // 每个表 执行sql
        String conditionSql = convertInfo.getConditionSql();
        String unionKey = MongoUtil.getUnionKey(relation);
        mysqlData.remove(unionKey);
        if(StringUtils.isBlank(conditionSql)) {
            conditionSql = " WHERE "+ unionKey+" = ?";
            convertInfo.setConditionSql(conditionSql);
        }
        String updateSql = doGetUpdateSql(convertInfo.getTableName(), mysqlData, conditionSql);
        DataSource dataSource = MysqlUtil.getDataSource(convertInfo.getWriteUrl(), convertInfo.getJobId());
        if (dataSource==null) {
            log.error("jobId:{}, 无法找到可用数据源", convertInfo.getJobId());
            return;
        }
        int update;
        int index = 0;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(updateSql);
            List<Object> valueParam = new ArrayList<>();
            index = setValues(index, preparedStatement, mysqlData, valueParam);
            preparedStatement.setObject(index + 1, idValue);
            log.info("updateSql:{}, value:{}, condition:{}", updateSql, valueParam, idValue);
            update = preparedStatement.executeUpdate();
        } catch (Exception e) {
            log.error("columns.size:{} index:{}, error: ", mysqlData.size(), index, e);
            throw new WatchException("sql 执行异常", e);
        } finally {
            try {
                assert connection != null;
                // 回收,并非关闭
                connection.close();
            } catch (SQLException e) {
                log.error("close Exception", e);
            }
        }
        log.info("statement.executeUpdate result:{}", update);
    }

    /**
     * 获取更新语句
     * @param tableName
     * @param data
     * @param conditionSql
     * @return
     */
    private static String doGetUpdateSql(String tableName, Map<String, Object> data, String conditionSql) {
        String updateSql;
        StringBuilder updateBuilder = new StringBuilder("UPDATE `" + tableName + "` SET");
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String column = entry.getKey();
            updateBuilder.append("`").append(column).append("`")
                    .append("=").append("?").append(",");
        }
        updateBuilder.delete(updateBuilder.length() - 1, updateBuilder.length());
        updateSql = updateBuilder.append(conditionSql).toString();
        return updateSql;
    }


    /**
     * 更新
     * @param convertInfo
     * @param id
     */
    public static void delete(ConvertInfo convertInfo, String id) {
        // 数据源中的各个表
        Map<String, String> relation = convertInfo.getTableColumns();
        // 每个表 执行sql
        String deleteSql = convertInfo.getDeleteSql();
        String unionKey = MongoUtil.getUnionKey(relation);
        if (StringUtils.isBlank(deleteSql)) {
            deleteSql = "DELETE FROM `" + convertInfo.getTableName() + "` WHERE " + unionKey + " = ?";
            convertInfo.setDeleteSql(deleteSql);
        }
        DataSource dataSource = MysqlUtil.getDataSource(convertInfo.getWriteUrl(), convertInfo.getJobId());
        if (dataSource==null) {
            log.error("jobId:{}, 无法找到可用数据源", convertInfo.getJobId());
            return;
        }
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(deleteSql);
            preparedStatement.setObject(1, id);
            log.info("deleteSql:{}, [{}:{}]", deleteSql, unionKey, id);
            int update = preparedStatement.executeUpdate();
            log.info("affected row:{}", update);
        } catch (Exception e) {
            log.error("delete error, id:{}", id, e);
        } finally {
            try {
                assert connection != null;
                connection.close();
            } catch (SQLException e) {
                log.error("close Exception", e);
            }
        }
    }

}
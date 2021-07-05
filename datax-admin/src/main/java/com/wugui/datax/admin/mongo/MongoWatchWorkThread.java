package com.wugui.datax.admin.mongo;

import com.mongodb.MongoInterruptedException;
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

    @Override
    public void run() {
        Thread currentThread = Thread.currentThread();
        currentThread.setName(String.format(ProjectConstant.MONGO_WATCH_INCREMENT_TASK_NAME, "changeStreamWorker", database, collection, THREAD_NUMBER.getAndIncrement()));
        String tableUnion = String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, connectUrl, database, collection);
        SpringContextHolder.getBean(MongoWatchWork.class).addTask(tableUnion, currentThread);
        int retry = 0;
        long sleepTime = 1000L;
        int connectionTry = JobAdminConfig.getAdminConfig().getMongoConnectionTry();
        String redisUnionTable = String.format(ProjectConstant.REDIS_UNION_COLLECTION__KEY_FORMAT, connectUrl, database, collection);
        MongoClient mongoClient;
        while (true) {
            try {
                Set<Integer> jobIds  = IncrementUtil.getCollectionJobsMap().get(tableUnion);
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
                    tableUnion = String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, connectUrl, database, collection);
                    jobIds  = IncrementUtil.getCollectionJobsMap().get(tableUnion);
                    if (CollectionUtils.isEmpty(jobIds)) {
                        log.error("jobIds is empty, finish the task");
                        redisTemplate.delete(redisUnionTable);
                        return;
                    }
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
            } catch (MongoInterruptedException e) {
                exit();
                return;
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
                    return;
                }
            }
        }
    }

    /**
     * 线程中断给予退出
     */
    private void exit() {
        log.info("thread is Interrupted, exit the thread");
        String tableUnion = String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, connectUrl, database, collection);
        SpringContextHolder.getBean(MongoWatchWork.class).closeTask(tableUnion);
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
        Set<Integer> jobIds  = IncrementUtil.getCollectionJobsMap().get(tableUnion);
        if (CollectionUtils.isEmpty(jobIds)) {
            return;
        }
        for (Integer jobId : jobIds) {
            ConvertInfo convertInfo = IncrementUtil.isContinue(jobId, OperationType.INSERT.name(), ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH, clusterTime.getTime() * 1000L, mongoData, null, id);
            if (convertInfo == null) {
                continue;
            }
            try {
                insert(convertInfo, mongoData);
            } catch (Exception e) {
                IncrementUtil.saveWaiting(jobId, OperationType.INSERT.name(), ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH, mongoData, null, id);
            }
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
        Set<Integer> jobIds  = IncrementUtil.getCollectionJobsMap().get(tableUnion);
        if (CollectionUtils.isEmpty(jobIds)) {
            return;
        }
        for (Integer jobId : jobIds) {
            ConvertInfo convertInfo = IncrementUtil.isContinue(jobId, OperationType.UPDATE.name(), ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH, clusterTime.getTime() * 1000L, mongoData, id, id);
            if (convertInfo == null) {
                continue;
            }
            try {
                update(convertInfo, mongoData, id);
            } catch (Exception e) {
                IncrementUtil.saveWaiting(jobId, OperationType.UPDATE.name(), ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH, mongoData, id, id);
            }
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
        Set<Integer> jobIds  = IncrementUtil.getCollectionJobsMap().get(tableUnion);
        if (CollectionUtils.isEmpty(jobIds)) {
            return;
        }
        for (Integer jobId : jobIds) {
            ConvertInfo convertInfo = IncrementUtil.isContinue(jobId, OperationType.DELETE.name(), ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH, clusterTime.getTime() * 1000L, null, id, id);
            if (convertInfo == null) {
                continue;
            }
            try {
                delete(convertInfo, id);
            } catch (Exception e) {
                IncrementUtil.saveWaiting(jobId, OperationType.DELETE.name(), ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH, null, id, id);
            }
        }
    }

    /**
     * 插入
     * @param convertInfo
     * @param mongoData
     */
    public static void insert(ConvertInfo convertInfo, Map<String, Object> mongoData) throws SQLException {
        DataSource dataSource = MysqlUtil.getDataSource(convertInfo.getWriteUrl(), convertInfo.getJobId());
        if (dataSource==null) {
            log.error("jobId:{}, 无法找到可用数据源", convertInfo.getJobId());
            return;
        }
        // 数据源中的各个表
        Map<String, String> relation = convertInfo.getTableColumns();
        Map<String, Object> mysqlData = columnConvert(mongoData, relation);
        // 每个表 执行sql
        String insertSql = doGetInsertSql(convertInfo.getTableName(), mysqlData);
        int update;
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
            throw e;
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
        StringBuilder placeholders = new StringBuilder(" VALUES(");
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
    public static void update(ConvertInfo convertInfo, Map<String, Object> updateData, String idValue) throws SQLException {
        DataSource dataSource = MysqlUtil.getDataSource(convertInfo.getWriteUrl(), convertInfo.getJobId());
        if (dataSource==null) {
            log.error("jobId:{}, 无法找到可用数据源", convertInfo.getJobId());
            return;
        }
        // 数据源中的各个表
        Map<String, String> relation = convertInfo.getTableColumns();

        Map<String, Object> mysqlData = columnConvert(updateData, relation);
        // 每个表 执行sql
        String unionKey = MongoUtil.getUnionKey(relation);
        if (StringUtils.isBlank(unionKey)) {
            log.info("主键不明，无法更新");
            return;
        }
        String conditionSql = MysqlUtil.doGetConditionSql(convertInfo, Collections.singleton(unionKey));
        mysqlData.remove(unionKey);
        if (mysqlData.isEmpty()) {
            log.info("无需更新");
            return;
        }
        String updateSql = MysqlUtil.doGetUpdateSql(convertInfo.getTableName(), mysqlData.keySet(), conditionSql);
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
            throw e;
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
     * 更新
     * @param convertInfo
     * @param id
     */
    public static void delete(ConvertInfo convertInfo, String id) throws SQLException {
        DataSource dataSource = MysqlUtil.getDataSource(convertInfo.getWriteUrl(), convertInfo.getJobId());
        if (dataSource==null) {
            log.error("jobId:{}, 无法找到可用数据源", convertInfo.getJobId());
            return;
        }
        // 数据源中的各个表
        Map<String, String> relation = convertInfo.getTableColumns();
        // 每个表 执行sql
        String unionKey = MongoUtil.getUnionKey(relation);
        if (StringUtils.isBlank(unionKey)) {
            log.info("主键不明，无法删除");
            return;
        }
        String deleteSql = MysqlUtil.doGetDeleteSql(convertInfo, Collections.singleton(unionKey));
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(deleteSql);
            preparedStatement.setObject(1, id);
            log.info("deleteSql:{}, [{}:{}]", deleteSql, unionKey, id);
            int update = preparedStatement.executeUpdate();
            log.info("affected row:{}", update);
        } catch (Exception e) {
            log.error("id:{},delete error:", id, e);
            throw e;
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
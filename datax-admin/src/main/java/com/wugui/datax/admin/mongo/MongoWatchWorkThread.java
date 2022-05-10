package com.wugui.datax.admin.mongo;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.wenwo.cloud.message.driven.producer.service.MessageProducerService;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.core.util.IncrementUtil;
import com.wugui.datax.admin.entity.ConvertInfo;
import com.wugui.datax.admin.util.MongoUtil;
import com.wugui.datax.admin.util.SpringContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    private final Map<Integer, List<MongoJobInfo>> jobs = new ConcurrentHashMap<>();

    private final MessageProducerService messageProducerService;

    /**
     * 构造函数
     * @param connectUrl
     * @param database
     * @param collection
     * @param messageProducerService
     */
    public MongoWatchWorkThread(String connectUrl, String database, String collection, MessageProducerService messageProducerService) {
        this.connectUrl = connectUrl;
        this.database = database;
        this.collection = collection;
        this.messageProducerService = messageProducerService;
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
        String redisUnionTable = String.format(ProjectConstant.REDIS_UNION_COLLECTION_KEY_FORMAT, connectUrl, database, collection);
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
                //异步处理各个任务
                if (!jobs.isEmpty()) {
                    jobs.forEach((k,v) -> {
                        if (!v.isEmpty()) {
                            messageProducerService.sendMsg(v, k.toString());
                        }
                    });
                    jobs.clear();
                }
            } catch (MongoInterruptedException e) {
                exit();
                return;
            }  catch (MongoCommandException e) {
                log.error("MongoCommandException:", e);
                if (260==e.getErrorCode()) {
                    redisTemplate.delete(redisUnionTable);
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
            List<MongoJobInfo> mongoJobInfos = getMongoJobInfos(jobId);
            MongoJobInfo mongoJobInfo = MongoJobInfo.builder().eventType(OperationType.INSERT).updateColumns(mongoData).idValue(id).build();
            mongoJobInfos.add(mongoJobInfo);
        }
    }

    /**
     * 获取记录
     * @param jobId
     * @return
     */
    private List<MongoJobInfo> getMongoJobInfos(Integer jobId) {
        List<MongoJobInfo> mongoJobInfos;
        if (jobs.containsKey(jobId)) {
            mongoJobInfos = jobs.get(jobId);
        } else {
            mongoJobInfos = new ArrayList<>();
            jobs.put(jobId, mongoJobInfos);
        }
        return mongoJobInfos;
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
            List<MongoJobInfo> mongoJobInfos = getMongoJobInfos(jobId);
            MongoJobInfo mongoJobInfo = MongoJobInfo.builder().eventType(OperationType.UPDATE).updateColumns(mongoData).id(id).idValue(id).build();
            mongoJobInfos.add(mongoJobInfo);
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
            List<MongoJobInfo> mongoJobInfos = getMongoJobInfos(jobId);
            MongoJobInfo mongoJobInfo = MongoJobInfo.builder().eventType(OperationType.DELETE).id(id).idValue(id).build();
            mongoJobInfos.add(mongoJobInfo);
        }
    }
}
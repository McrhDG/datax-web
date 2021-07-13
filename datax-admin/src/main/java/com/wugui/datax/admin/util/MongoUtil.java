package com.wugui.datax.admin.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Maps;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.core.util.IncrementUtil;
import com.wugui.datax.admin.entity.JobInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <pre>
 *
 * @author ruanjl jiangling.ruan@pingcl.com
 * @since 2021/3/5 18:32
 */
@Slf4j
public class MongoUtil {

    /**
     * 私有构造
     */
    private MongoUtil() {
    }

    /**
     * mongo id
     */
    public static final String MONGO_ID = "_id";

    public static final String MONGODB_PREFIX = "mongodb://";

    /**
     * mysql数据源
     */
    private static final Map<String, MongoClient> MONGO_CLIENT_MAP = new ConcurrentHashMap<>();

    /**
     * 获取唯一id键
     * @param relation
     * @return
     */
    public static String getUnionKey(Map<String, String> relation) {
        String unionKey = relation.get(MONGO_ID);
        if (StringUtils.isBlank(unionKey)) {
            unionKey = "id";
        }
        return unionKey;
    }

    /**
     * @param fullDocument
     * @return
     */
    public static Map<String, Object> toParseMap(Map<String, Object> fullDocument, BsonDocument documentKey) {
        Map<String, Object> map = Maps.newHashMapWithExpectedSize(fullDocument.size());
        fullDocument.forEach((key, value) -> {
            if (MONGO_ID.equals(key)) {
                map.put(MONGO_ID, getDocumentKeyId(documentKey));
            } else {
                if (value instanceof Collection) {
                    map.put(key, JSON.toJSONString(value));
                }else if (value instanceof Document) {
                    map.put(key, JSON.toJSONString(value));
                } else {
                    map.put(key, value);
                }
            }
        });
        return map;
    }

    /**
     * 获取主键
     * @param bsonDocument
     * @return
     */
    public static Map<String, Object> toParseMap(BsonDocument bsonDocument) {
        Map<String, Object> map = Maps.newHashMapWithExpectedSize(bsonDocument.size());

        bsonDocument.forEach((key, value) -> {
            Object convertValue;
            if (bsonDocument.isObjectId(key)) {
                convertValue =  bsonDocument.getObjectId(key).getValue().toString();
            } else if (bsonDocument.isString(key)) {
                convertValue =  bsonDocument.getString(key).getValue();
            } else if (bsonDocument.isInt64(key)) {
                convertValue = bsonDocument.getInt64(key).getValue();
            } else if (bsonDocument.isNull(key)) {
                convertValue = null;
            } else if (bsonDocument.isInt32(key)) {
                convertValue = bsonDocument.getInt32(key).getValue();
            } else if (bsonDocument.isDecimal128(key)) {
                convertValue = bsonDocument.getDecimal128(key).getValue();
            } else if (bsonDocument.isDouble(key)) {
                convertValue = bsonDocument.getDouble(key).getValue();
            } else if (bsonDocument.isBoolean(key)) {
                convertValue = bsonDocument.getBoolean(key).getValue();
            } else if (bsonDocument.isDateTime(key)) {
                convertValue = new Date(bsonDocument.getDateTime(key).getValue());
            } else if (bsonDocument.isTimestamp(key)) {
                convertValue = new Date(bsonDocument.getTimestamp(key).getValue());
            } else {
                convertValue = bsonDocument.getString(key).getValue();
            }
            map.put(key, convertValue);
        });
        return map;

    }

    /**
     * 获取主键
     * @param documentKey
     * @return
     */
    public static String getDocumentKeyId(BsonDocument documentKey) {
        if (documentKey.isObjectId(MONGO_ID)) {
            return documentKey.getObjectId(MONGO_ID).getValue().toString();
        }

        if (documentKey.isString(MONGO_ID)) {
            return documentKey.getString(MONGO_ID).getValue();
        }

        if (documentKey.isInt64(MONGO_ID)) {
            return String.valueOf(documentKey.getInt64(MONGO_ID).getValue());
        }

        if (documentKey.isInt32(MONGO_ID)) {
            return String.valueOf(documentKey.getInt32(MONGO_ID).getValue());
        }
        return null;
    }

    /**
     * 添加数据源
     */
    public static MongoClient addMongoClient(JobInfo jobInfo) {

        JSONObject content = IncrementUtil.getContent(jobInfo);
        JSONObject reader = content.getJSONObject("reader");
        String readerName = reader.getString("name");
        if (!ProjectConstant.MONGODB_READER.equalsIgnoreCase(readerName)) {
            return null;
        }
        JSONObject readerParam = reader.getJSONObject("parameter");
        Set<String> addressSet = readerParam.getObject("address", new TypeReference<Set<String>>(){});
        String address = StringUtils.join(addressSet, ',');
        String mongoUrl = MONGODB_PREFIX;
        String readerUsername = readerParam.getString("userName");
        readerUsername = AESUtil.decrypt(readerUsername);
        String readerPassword = readerParam.getString("userPassword");
        readerPassword = AESUtil.decrypt(readerPassword);
        if (StringUtils.isNotBlank(readerUsername) && StringUtils.isNotBlank(readerPassword)) {
            mongoUrl += readerUsername + ":" + readerPassword + "@";
        }
        mongoUrl += address;
        MongoClient mongoClient = MongoClients.create(mongoUrl);
        MONGO_CLIENT_MAP.put(address, mongoClient);
        return mongoClient;
    }

    /**
     * 获取数据源
     * @param address
     * @param jobId
     * @return
     */
    public static MongoClient getMongoClient(String address, int jobId) {
        MongoClient mongoClient =  MONGO_CLIENT_MAP.get(address);
        if (mongoClient==null) {
            JobInfo jobInfo = JobAdminConfig.getAdminConfig().getJobInfoMapper().loadById(jobId);
            if (jobInfo == null) {
                log.error(">>>>>>>>>>>> trigger fail, jobId invalid，jobId={}", jobId);
                return null;
            }
            mongoClient = addMongoClient(jobInfo);
        }
        return mongoClient;
    }
}

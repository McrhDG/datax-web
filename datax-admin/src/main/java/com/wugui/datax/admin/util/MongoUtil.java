package com.wugui.datax.admin.util;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Maps;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.core.util.IncrementUtil;
import com.wugui.datax.admin.entity.ConvertInfo;
import com.wugui.datax.admin.entity.JobInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
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
    public static String getUnionKey(Map<String, ConvertInfo.ToColumn> relation) {
        if (relation.get(MONGO_ID)==null || StringUtils.isBlank(relation.get(MONGO_ID).getName())) {
            return  "id";
        }
        return relation.get(MONGO_ID).getName();
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
                map.put(key, value);
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

        bsonDocument.forEach((key, value) -> map.put(key, toParse(value)));
        return map;

    }


    /**
     * 装换
     * @param bsonValue
     * @return
     */
    public static Object toParse(BsonValue bsonValue) {
        Object value = null;
        if (bsonValue.isObjectId()) {
            value =  bsonValue.asObjectId().getValue().toString();
        } else if (bsonValue.isString()) {
            value =  bsonValue.asString().getValue();
        } else if (bsonValue.isInt64()) {
            value = bsonValue.asInt64().getValue();
        }  else if (bsonValue.isInt32()) {
            value = bsonValue.asInt32().getValue();
        } else if (bsonValue.isDecimal128()) {
            value = bsonValue.asDecimal128().getValue().doubleValue();
        } else if (bsonValue.isDouble()) {
            value = bsonValue.asDouble().getValue();
        } else if (bsonValue.isBoolean()) {
            value = bsonValue.asBoolean().getValue();
        } else if (bsonValue.isDateTime()) {
            value = new Date(bsonValue.asDateTime().getValue());
        } else if (bsonValue.isTimestamp()) {
            value = new Date(bsonValue.asTimestamp().getValue());
        } else if (bsonValue.isDocument()) {
            BsonDocument document = bsonValue.asDocument();
            if (document!=null) {
                value = toParseMap(document);
            }
        } else if (bsonValue.isArray()) {
            BsonArray array = bsonValue.asArray();
            if (!CollectionUtils.isEmpty(array)) {
                List<Object> list = new ArrayList<>();
                for (BsonValue bsonValue1 : array) {
                    list.add(toParse(bsonValue1));
                }
                value = list;
            }
        }
        return value;
    }


    /**
     * 是否基础类型
     * @param clz
     * @return
     */
    public static boolean isBaseType(Class<?> clz){
        return ClassUtils.isPrimitiveOrWrapper(clz) || clz==String.class || clz==Date.class || clz== LocalDate.class || clz== LocalDateTime.class || clz.getPackage().getName().startsWith("java.");
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

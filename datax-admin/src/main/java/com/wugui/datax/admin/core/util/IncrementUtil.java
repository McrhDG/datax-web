package com.wugui.datax.admin.core.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Maps;
import com.wugui.datax.admin.canal.DataSourceFactory;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.entity.ConvertInfo;
import com.wugui.datax.admin.entity.JobInfo;
import com.wugui.datax.admin.mongo.MongoWatchWork;
import com.wugui.datax.admin.util.AESUtil;
import com.wugui.datax.admin.util.MysqlUtil;
import com.wugui.datax.admin.util.SpringContextHolder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
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

    private static final Logger logger = LoggerFactory.getLogger(IncrementUtil.class);

    /**
     * 任务装换关系
     */
    private static final Map<Integer, ConvertInfo> CONVERT_INFO_MAP = new ConcurrentHashMap<>();

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
            // reader
            JSONObject reader = content.getJSONObject("reader");
            String name = reader.getString("name");
            if (ProjectConstant.MYSQL_READER.equalsIgnoreCase(name)) {
                initCanal(jobInfo, isInit);
            } else if (ProjectConstant.MONGODB_READER.equalsIgnoreCase(name)) {
                initMongoWatch(jobInfo, isInit);
            }
        } catch (Exception e) {
            logger.error("initIncrementData error:", e);
        }
    }

    /**
     * @param jobInfo
     * @param isInit
     */
    public static void initCanal(JobInfo jobInfo, boolean isInit) {

        if (jobInfo.getIncrementType()>0) {
            logger.info("jobId:{}, 存在自增设置，无需继续处理增量数据", jobInfo.getId());
            return;
        }

        String encryptJobJson = jobInfo.getJobJson();
        JSONObject jsonObj = JSONObject.parseObject(encryptJobJson);
        JSONObject jobJson = jsonObj.getJSONObject("job");
        JSONArray contents = jobJson.getJSONArray("content");
        JSONObject content = (JSONObject) contents.get(0);

        JSONObject reader = content.getJSONObject("reader");
        String name = reader.getString("name");
        if (!ProjectConstant.MYSQL_READER.equalsIgnoreCase(name)) {
            return;
        }
        JSONObject readerParam = reader.getJSONObject("parameter");

        JSONArray readerColumnArray = readerParam.getJSONArray("column");
        JSONArray readerConnections = readerParam.getJSONArray("connection");
        JSONObject readerConnectionJsonObj = (JSONObject) readerConnections.get(0);
        String querySql = readerConnectionJsonObj.getString("querySql");
        if (StringUtils.isNotBlank(querySql)) {
            logger.info("jobId:{}, 存在自定义sql，无需继续处理增量数据", jobInfo.getId());
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
            logger.info("jobId:{}, 字段无法对应，无法继续处理增量数据", jobInfo.getId());
            return;
        }

        String readerJdbcUrl = (String)readerConnectionJsonObj.getJSONArray("jdbcUrl").get(0);
        String readerTable = (String)readerConnectionJsonObj.getJSONArray("table").get(0);

        String readerDataBase = MysqlUtil.getMysqlDataBase(readerJdbcUrl);

        // 获取到写入的库table 丢给canal一个增量同步任务

        String username = writerParam.getString("username");
        username = AESUtil.decrypt(username);
        String password = writerParam.getString("password");
        password = AESUtil.decrypt(password);
        JSONArray writerConnections = writerParam.getJSONArray("connection");
        JSONObject writerConnectionJsonObj = (JSONObject) writerConnections.get(0);
        String writeTable = writerConnectionJsonObj.getJSONArray("table").getString(0);
        String writerJdbcUrl = writerConnectionJsonObj.getString("jdbcUrl");
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(writerJdbcUrl);
        druidDataSource.setUsername(username);
        druidDataSource.setPassword(password);

        // 写入的时表名可能不一样 表名可能需要转化
        DataSourceFactory.instance().putConvert(readerTable, writeTable);
        // canal监听的库和表
        String dataBaseTable = readerDataBase + "|" + readerTable;
        DataSourceFactory.instance().putTableColumn(dataBaseTable, readerColumnArray, writerColumnArray);
        // 重启初始化
        if (isInit) {
            Long canalTimestamp = jobInfo.getIncrementSyncTime().getTime();
            DataSourceFactory.instance().addNewTask(dataBaseTable, druidDataSource, canalTimestamp);
            return;
        }
        // 首次初始化
        long initTimestamp = System.currentTimeMillis();
        // 添加新的canal任务
        DataSourceFactory.instance().addNewTask(dataBaseTable, druidDataSource, initTimestamp);

        // 添加 创建时间的where 语句 并保存起来
        String initDateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(initTimestamp);

        String whereClause = "create_time < " + "'" + initDateStr + "'";
        readerParam.put("where", whereClause);

        reader.put("parameter", readerParam);
        content.put("reader", reader);
        jobInfo.setIncrementSyncType(ProjectConstant.INCREMENT_SYNC_TYPE.CANAL.val());
        jobInfo.setIncrementSyncTime(new Date(initTimestamp));
        contents.add(0, content);
        jobJson.put("content", contents);
        jsonObj.put("job", jobJson);
        jobInfo.setJobJson(jsonObj.toJSONString());
        JobAdminConfig.getAdminConfig().getJobInfoMapper().update(jobInfo);
    }

    /**
     *获取json内容
     * @param jobInfo
     * @return
     */
    public static JSONObject getContent(JobInfo jobInfo) {
        String encryptJobJson = jobInfo.getJobJson();
        JSONObject jsonObj = JSONObject.parseObject(encryptJobJson);
        JSONObject jobJson = jsonObj.getJSONObject("job");
        JSONArray contents = jobJson.getJSONArray("content");
        JSONObject content = contents.getJSONObject(0);
        return content;
    }



    /**
     * @param jobInfo
     */
    public static void initMongoWatch(JobInfo jobInfo, boolean isInit) {

        if (jobInfo.getIncrementType()>0) {
            logger.info("jobId:{}, 存在自增设置，无需继续处理增量数据", jobInfo.getId());
            return;
        }

        JSONObject content = IncrementUtil.getContent(jobInfo);

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
            logger.info("jobId:{}, 字段无法对应，无法继续处理增量数据", jobInfo.getId());
            return;
        }

        String readerDataBase = readerParam.getString("dbName");
        String collection = readerParam.getString("collectionName");


        //表-任务
        Set<String> addressSet = readerParam.getObject("address", new TypeReference<Set<String>>(){});
        String address = StringUtils.join(addressSet, ',');
        String tableUnion = String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, address, readerDataBase, collection);
        IncrementUtil.putTableJob(tableUnion, jobInfo.getId());

        //装换关系
        Map<String, String> convertTableColumn = mongoConvertTableColumn(readerColumnArray, writerColumnArray);
        JSONArray writerConnections = writerParam.getJSONArray("connection");
        JSONObject writerConnectionJsonObj = (JSONObject) writerConnections.get(0);
        String writerJdbcUrl = writerConnectionJsonObj.getString("jdbcUrl");
        String writerUrl = MysqlUtil.getMysqlUrl(writerJdbcUrl);
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

        String writeTable = writerConnectionJsonObj.getJSONArray("table").getString(0);
        ConvertInfo convertInfo = new ConvertInfo(jobInfo.getId(), writeTable, convertTableColumn, writerUrl, initTimestamp);
        IncrementUtil.putConvertInfo(jobInfo.getId(), convertInfo);

        MongoWatchWork mongoWatchWork = SpringContextHolder.getBean(MongoWatchWork.class);
        mongoWatchWork.addTask(tableUnion);
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
     * mysql字段转换关系
     * @param readerColumn
     * @param writerColumn
     */
    public static Map<String,String>  mysqlConvertTableColumn(JSONArray readerColumn, JSONArray writerColumn) {
        Map<String,String> map = Maps.newHashMapWithExpectedSize(readerColumn.size());
        for (int i = 0; i < readerColumn.size(); i++) {
            String key = (String) readerColumn.get(i);
            String value = (String) writerColumn.get(i);
            if(StringUtils.isNotBlank(value)) {
                map.put(key.replace("`", ""), value.replace("`", ""));
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
    public static Map<String,String> mongoConvertTableColumn(JSONArray readerColumn, JSONArray writerColumn) {
        Map<String,String> map = Maps.newHashMapWithExpectedSize(readerColumn.size());
        for (int i = 0; i < readerColumn.size(); i++) {
            JSONObject jObj = readerColumn.getJSONObject(i);
            String key = jObj.getString("name");
            String value = (String) writerColumn.get(i);
            if(StringUtils.isNotBlank(value)) {
                map.put(key, value.replace("`", ""));
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

            JSONObject reader = content.getJSONObject("reader");
            String readerName = reader.getString("name");
            if (ProjectConstant.MONGODB_READER.equalsIgnoreCase(readerName)) {
                JSONObject readerParam = reader.getJSONObject("parameter");
                String readerDataBase = readerParam.getString("dbName");
                String collection = readerParam.getString("collectionName");

                Set<String> addressSet = readerParam.getObject("address", new TypeReference<Set<String>>(){});
                String address = StringUtils.join(addressSet, ',');
                String tableUnion = String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, address, readerDataBase, collection);
                if (TABLE_JOBS_MAP.containsKey(tableUnion)) {
                    Set<Integer> jobIds = TABLE_JOBS_MAP.get(tableUnion);
                    if (!jobIds.isEmpty()) {
                        jobIds.remove(jobInfo.getId());
                    }

                    if (jobIds.isEmpty()) {
                        TABLE_JOBS_MAP.remove(tableUnion);
                        MongoWatchWork mongoWatchWork = SpringContextHolder.getBean(MongoWatchWork.class);
                        mongoWatchWork.removeTask(tableUnion);
                    }
                }
            } else if(ProjectConstant.MYSQL_READER.equalsIgnoreCase(readerName)) {
                /*JSONObject readerParam = reader.getJSONObject("parameter");
                JSONArray readerConnections = readerParam.getJSONArray("connection");
                JSONObject readerConnectionJsonObj = (JSONObject) readerConnections.get(0);
                String readerJdbcUrl = readerConnectionJsonObj.getJSONArray("jdbcUrl").getString(0);
                String readerAddress = MysqlUtil.getMysqlAddress(readerJdbcUrl);
                String readerDataBase = MysqlUtil.getMysqlDataBase(readerJdbcUrl);
                String readerTable = readerConnectionJsonObj.getJSONArray("table").getString(0);
                tableUnion = String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, readerAddress, readerDataBase, readerTable);*/
                JSONObject readerParam = reader.getJSONObject("parameter");
                JSONArray readerConnections = readerParam.getJSONArray("connection");
                JSONObject readerConnectionJsonObj = (JSONObject) readerConnections.get(0);
                String readerJdbcUrl = (String)readerConnectionJsonObj.getJSONArray("jdbcUrl").get(0);
                String readerTable = (String)readerConnectionJsonObj.getJSONArray("table").get(0);

                String readerDataBase = MysqlUtil.getMysqlDataBase(readerJdbcUrl);
                // canal监听的库和表
                String dataBaseTable = readerDataBase + "|" + readerTable;
                DataSourceFactory.instance().closeTask(dataBaseTable);
            }
        } catch (Exception e) {
            logger.error("removeTask error:", e);
        }
    }
}

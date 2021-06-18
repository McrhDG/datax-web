package com.wugui.datax.admin.core.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wugui.datax.admin.canal.DataSourceFactory;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.entity.JobInfo;
import com.wugui.datax.admin.util.AESUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

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
     * 初始化增量同步器
     * @param jobInfo
     */
    public static void initIncrementData(JobInfo jobInfo) {
        String encryptJobJson = jobInfo.getJobJson();
        JSONObject jsonObj = JSONObject.parseObject(encryptJobJson);
        JSONObject jobJson = jsonObj.getJSONObject("job");
        JSONArray contents = jobJson.getJSONArray("content");
        JSONObject content = (JSONObject) contents.get(0);
        // reader
        JSONObject reader = content.getJSONObject("reader");
        String name = reader.getString("name");
        if (ProjectConstant.MYSQL_READER.equalsIgnoreCase(name)) {
            initCanal(jobInfo);
        } else if (ProjectConstant.MONGODB_READER.equalsIgnoreCase(name)) {
            //TODO 2.0 接入mongdb watch
        }
    }

    /**
     * @param jobInfo
     */
    public static void initCanal(JobInfo jobInfo) {

        if (jobInfo.getIncrementType()>0) {
            logger.info("存在自增设置，无需继续处理增量数据");
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
            logger.info("存在自定义sql，无需继续处理增量数据");
            return;
        }
        String readerJdbcUrl = (String)readerConnectionJsonObj.getJSONArray("jdbcUrl").get(0);
        String readerTable = (String)readerConnectionJsonObj.getJSONArray("table").get(0);
        String readerDataBase;
        if (readerJdbcUrl.contains("?")) {
            readerDataBase = readerJdbcUrl.replaceAll("jdbc:mysql://.*?:.*?/(.*?)\\?.*", "$1");
        } else {
            readerDataBase = readerJdbcUrl.replaceAll("jdbc:mysql://.*?:.*?/(.*)", "$1");
        }

        // 获取到写入的库table 丢给canal一个增量同步任务
        // writer
        JSONObject writer = content.getJSONObject("writer");
        JSONObject writerParam = writer.getJSONObject("parameter");
        JSONArray writerColumnArray = writerParam.getJSONArray("column");
        String username = writerParam.getString("username");
        username = AESUtil.decrypt(username);
        String password = writerParam.getString("password");
        password = AESUtil.decrypt(password);
        JSONArray writerConnections = writerParam.getJSONArray("connection");
        JSONObject writerConnectionJsonObj = (JSONObject) writerConnections.get(0);
        String writeTable = (String)writerConnectionJsonObj.getJSONArray("table").get(0);
        String writerJdbcUrl = writerConnectionJsonObj.getString("jdbcUrl");
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(writerJdbcUrl);
        druidDataSource.setUsername(username);
        druidDataSource.setPassword(password);

        // 写入的时表名可能不一样 表名可能需要转化
        DataSourceFactory.instance().putConvert(readerTable, writeTable);
        // canal监听的库和表
        String dataBaseTable = readerDataBase + "|" + readerTable;
        //列对应的关系
        if(readerColumnArray.size() > writerColumnArray.size()) {
            logger.info("字段无法对应，无法继续处理增量数据");
            return;
        }
        DataSourceFactory.instance().putTableColumn(dataBaseTable, readerColumnArray, writerColumnArray);
        Long canalTimestamp = jobInfo.getCanalTimestamp();
        // 重启初始化
        if (canalTimestamp != null) {
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

        jobInfo.setCanalTimestamp(initTimestamp);
        contents.add(0, content);
        jobJson.put("content", contents);
        jsonObj.put("job", jobJson);
        jobInfo.setJobJson(jsonObj.toJSONString());
        JobAdminConfig.getAdminConfig().getJobInfoMapper().update(jobInfo);
    }

}

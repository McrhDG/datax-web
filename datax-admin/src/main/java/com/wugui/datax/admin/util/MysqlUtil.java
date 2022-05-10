package com.wugui.datax.admin.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.util.IncrementUtil;
import com.wugui.datax.admin.entity.ConvertInfo;
import com.wugui.datax.admin.entity.JobInfo;
import com.wugui.datax.admin.mapper.JobInfoMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * mysql通用类
 *
 * @author chen.ruihong
 * @version 1.0
 * @date 2021/6/23 20:03
 */
@Slf4j
public class MysqlUtil {

    /**
     * 私有构造
     */
    private MysqlUtil() {
    }

    /**
     * mysql 的 uri 正则
     */
    public static final String MYSQL_JDBC_URI_REGEX = "jdbc:mysql://((.+?)/(\\w+))(\\?.*)?";

    /**
     * mysql url正则表达式
     */
    private static final Pattern MYSQL_JDBC_URI_PATTERN = Pattern.compile(MYSQL_JDBC_URI_REGEX);

    /**
     * mysql数据源
     */
    private static final Map<String, DruidDataSource> DATASOURCE_MAP = new ConcurrentHashMap<>();


    private static final String REWRITE_BATCHED_STATEMENTS = "rewriteBatchedStatements";

    private static final String ALLOW_MULTI_QUERIES = "allowMultiQueries";


    /**
     *
     * @param jdbcUrl
     * @return
     */
    public static String getMysqlDataBase(String jdbcUrl) {
        Matcher matcher = MYSQL_JDBC_URI_PATTERN.matcher(jdbcUrl);
        if(matcher.find()) {
            return matcher.group(3);
        }
        return null;
    }

    /**
     * 获取连接数据库
     * @param jdbcUrl
     * @return
     */
    public static String getMysqlUrl(String jdbcUrl) {
        Matcher matcher = MYSQL_JDBC_URI_PATTERN.matcher(jdbcUrl);
        if(matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    /**
     *
     * @param jdbcUrl
     * @return
     */
    public static String getMysqlAddress(String jdbcUrl) {
        Matcher matcher = MYSQL_JDBC_URI_PATTERN.matcher(jdbcUrl);
        if(matcher.find()) {
            return matcher.group(2).replace(".", "_").replace(":", "_");
        }
        return null;
    }

    /**
     * 添加数据源
     */
    public static DruidDataSource addDataSource(JobInfo jobInfo) {

        JSONObject content = IncrementUtil.getContent(jobInfo);
        // writer
        JSONObject writer = content.getJSONObject("writer");
        String writerName = writer.getString("name");
        if (!ProjectConstant.MYSQL_WRITER.equalsIgnoreCase(writerName)) {
            return null;
        }

        JSONObject writerParam = writer.getJSONObject("parameter");
        String username = writerParam.getString("username");
        username = AESUtil.decrypt(username);
        String password = writerParam.getString("password");
        password = AESUtil.decrypt(password);
        JSONArray writerConnections = writerParam.getJSONArray("connection");
        JSONObject writerConnectionJsonObj = (JSONObject) writerConnections.get(0);
        //添加数据源
        String writerJdbcUrl = writerConnectionJsonObj.getString("jdbcUrl");
        String writerUrl = MysqlUtil.getMysqlUrl(writerJdbcUrl);
        if (!writerJdbcUrl.contains(REWRITE_BATCHED_STATEMENTS)){
            writerJdbcUrl += (writerJdbcUrl.contains("?")?"&":"?") + "rewriteBatchedStatements=true";
        }
        if (!writerJdbcUrl.contains(ALLOW_MULTI_QUERIES)){
            writerJdbcUrl += (writerJdbcUrl.contains("?")?"&":"?") + "allowMultiQueries=true";
        }
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(writerJdbcUrl);
        druidDataSource.setUsername(username);
        druidDataSource.setPassword(password);
        DATASOURCE_MAP.put(writerUrl, druidDataSource);
        return druidDataSource;
    }

    /**
     * 获取数据源
     * @param address
     * @param jobId
     * @return
     */
    public static DruidDataSource getDataSource(String address, int jobId) {
        DruidDataSource druidDataSource =  DATASOURCE_MAP.get(address);
        if (druidDataSource==null) {
            JobInfo jobInfo = SpringContextHolder.getBean(JobInfoMapper.class).loadById(jobId);
            if (jobInfo == null) {
                log.error(">>>>>>>>>>>> trigger fail, jobId invalid，jobId={}", jobId);
                return null;
            }
            druidDataSource = addDataSource(jobInfo);
        }
        return druidDataSource;
    }


    /**
     * 获取条件语句
     * @param convertInfo
     * @param conditionColumns
     * @return
     */
    public static String doGetConditionSql(ConvertInfo convertInfo, Set<String> conditionColumns) {
        String conditionSql = convertInfo.getConditionSql();
        if(StringUtils.isBlank(conditionSql)) {
            StringBuilder conditionBuilder = new StringBuilder(" WHERE ");
            conditionColumns.forEach(column -> conditionBuilder.append("`").append(column).append("`").append("=").append("%s").append(" AND"));
            conditionBuilder.delete(conditionBuilder.length() - 4, conditionBuilder.length());
            conditionSql = conditionBuilder.toString();
            convertInfo.setConditionSql(conditionSql);
        }
        return conditionSql;
    }
}

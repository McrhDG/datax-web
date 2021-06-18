package com.wugui.datax.admin.canal;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * <pre>
 * canal 数据库增量同步工厂
 *
 * @author ruanjl ruanjlee@gmail.com
 * @since 2020/9/22 16:22
 */
@Slf4j
public class DataSourceFactory {

    private DataSourceFactory() {
    }

    private static class DataSourceHolder{
        private static final DataSourceFactory INSTANCE = new DataSourceFactory();
    }

    public static DataSourceFactory instance(){
        return DataSourceHolder.INSTANCE;
    }

    /**
     * 数据源缓存
     */
    private final ConcurrentMap<String, DruidDataSource> dataSourceCache = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Long> taskInitTimeStamp = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, String> TABLE_NAME_CONVERT = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Map<String, String>> TABLE_COLUMN_CONVERT = new ConcurrentHashMap<>();

    /**
     * @param dataBaseTable
     * @return 数据写入的DataSource
     */
    public DruidDataSource getDataSource(String dataBaseTable) {
        return dataSourceCache.get(dataBaseTable);
    }

    public Long getInitTimestamp(String dataBaseTable) {
        return taskInitTimeStamp.get(dataBaseTable);
    }

    /**
     *  @param dataBaseTable 数据来源库的:  库名 | 表名
     * @param dataSource 数据写入库的:  dataSource
     * @param timestamp  canal初始化的时间
     */
    public void addNewTask(String dataBaseTable, DruidDataSource dataSource, Long timestamp) {
        dataSourceCache.put(dataBaseTable, dataSource);
        taskInitTimeStamp.put(dataBaseTable, timestamp);
    }

    public boolean isEmpty(){
        return dataSourceCache.isEmpty();
    }

    public String convertTableName(String from) {
        return TABLE_NAME_CONVERT.get(from);
    }

    public void putConvert(String from, String to) {
        TABLE_NAME_CONVERT.put(from, to);
    }

    public Map<String,String> convertTableColumn(String dataBaseTable) {
        return TABLE_COLUMN_CONVERT.get(dataBaseTable);
    }

    /**
     * 字段转换关系
     * @param dataBaseTable
     * @param readerColumn
     * @param writerColumn
     */
    public void putTableColumn(String dataBaseTable, List<Object> readerColumn, List<Object> writerColumn) {
        Map<String,String> map = new HashMap<>(16);
        for (int i = 0; i < readerColumn.size(); i++) {
            String key = (String) readerColumn.get(i);
            String value = (String) writerColumn.get(i);
            if(StringUtils.isNotBlank(value)) {
                map.put(key.replace("`", ""), value.replace("`", ""));
            }
        }
        if (!map.isEmpty()) {
            TABLE_COLUMN_CONVERT.put(dataBaseTable, map);
        }
    }

}

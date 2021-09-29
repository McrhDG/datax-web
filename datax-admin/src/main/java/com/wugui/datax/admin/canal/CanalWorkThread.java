package com.wugui.datax.admin.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.util.IncrementUtil;
import com.wugui.datax.admin.entity.ConvertInfo;
import com.wugui.datax.admin.util.DateUtil;
import com.wugui.datax.admin.util.MysqlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据同步线程父类
 * @author chen.ruihong
 * @date 2021-06-24
 */
@Slf4j
public class CanalWorkThread extends Thread {

    private static final AtomicInteger THREAD_NUMBER = new AtomicInteger(1);

    private static final String CONTEXT_FORMAT = SystemUtils.LINE_SEPARATOR
                + "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SystemUtils.LINE_SEPARATOR
                + "* Start : [{}]  End : [{}] "
                + SystemUtils.LINE_SEPARATOR;

    private static final String ROW_FORMAT =  SystemUtils.LINE_SEPARATOR
            + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {}({}) , gtid : ({}) , delay : {} ms"
            + SystemUtils.LINE_SEPARATOR;

    private static final String TRANSACTION_FORMAT = SystemUtils.LINE_SEPARATOR
            + "================> binlog[{}:{}] , executeTime : {}({}) , gtid : ({}) , delay : {}ms"
            + SystemUtils.LINE_SEPARATOR;

    private CanalConnector connector;

    private final String address;

    private volatile boolean running = true;

    public void noRun() {
        this.running = false;
    }

    /**
     * 构造函数
     * @param address
     */
    public CanalWorkThread(String address) {
        this.address = address;
    }


    @Override
    public void run() {
        String destination = InstanceFactory.getDestination(address);
        Thread currentThread = Thread.currentThread();
        currentThread.setName(String.format(ProjectConstant.CANAL_INCREMENT_TASK_NAME, "canalWorkThread", destination, THREAD_NUMBER.getAndIncrement()));
        int round = 0;
        int batchSize = 5 * 1024;
        int retry = 0;
        long sleepTime = 1000L;
        while (running) {
            try {
                connector = InstanceFactory.getCanalConnector(address);
                MDC.put("destination", destination);
                connector.connect();
                log.info("connect to canalServerAddress:{}", InstanceFactory.getCanalConnectionAddress(address));
                connector.subscribe();
                while (running) {
                    // 获取指定数量的数据
                    Message message = connector.getWithoutAck(batchSize);
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        Thread.sleep(InstanceFactory.getPullInterval(address));
                        if (round++ % 300 == 0) {
                            log.info("sleeping--------");
                        }
                    } else {
                        printSummary(message, batchId, size);
                        // 操作入口
                        printEntry(message.getEntries());
                    }
                    if (batchId != -1) {
                        // 提交确认
                        connector.ack(batchId);
                    }
                }
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
                return;
            } catch (CanalClientException e) {
                log.error("CanalClientException error!", e);
                if (e.getCause() instanceof InterruptedException) {
                    return;
                }
            } catch (Throwable e) {
                log.error("process error!", e);
                if (e.getCause() instanceof InterruptedException) {
                    return;
                }
                // 处理失败, 回滚数据
                try {
                    if (connector.checkValid()) {
                        connector.rollback();
                    }
                } catch (Throwable canalClientException) {
                    log.error("process error!", e);
                }
                try {
                    if (retry < InstanceFactory.getConnectionRetry(address)) {
                        retry ++;
                        sleepTime = sleepTime * retry;
                    }
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e1) {
                    log.error("InterruptedException", e1);
                    return;
                }
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    private void printSummary(Message message, long batchId, int size) {
        long memsize = 0;
        for (CanalEntry.Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        log.info(CONTEXT_FORMAT, batchId, size, memsize, DateUtil.format(), startPosition, endPosition);
    }

    private String buildPositionForDump(CanalEntry.Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        String position = entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
                + time + "(" + DateUtil.format(time) + ")";
        if (StringUtils.isNotEmpty(entry.getHeader().getGtid())) {
            position += " gtid(" + entry.getHeader().getGtid() + ")";
        }
        return position;
    }

    private void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = System.currentTimeMillis() - executeTime;

            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                    TransactionBegin begin;
                    try {
                        begin = TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    log.info(TRANSACTION_FORMAT,
                            entry.getHeader().getLogfileName(),
                            entry.getHeader().getLogfileOffset(),
                            executeTime, DateUtil.format(executeTime),
                            entry.getHeader().getGtid(), delayTime);
                    log.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
                    printXAInfo(begin.getPropsList());
                } else {
                    TransactionEnd end;
                    try {
                        end = TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    log.info(" END ----> transaction id: {}", end.getTransactionId());
                    printXAInfo(end.getPropsList());
                    log.info(TRANSACTION_FORMAT,
                            entry.getHeader().getLogfileName(),
                            entry.getHeader().getLogfileOffset(),
                            executeTime, DateUtil.format(executeTime),
                            entry.getHeader().getGtid(), delayTime);
                }
                continue;
            }

            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChange;
                try {
                    rowChange = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
                CanalEntry.EventType eventType = rowChange.getEventType();
                String sql = rowChange.getSql();
                if (eventType == EventType.QUERY) {
                    continue;
                }
                String dataBase = entry.getHeader().getSchemaName();
                String tableName = entry.getHeader().getTableName();
                log.info(ROW_FORMAT,
                        entry.getHeader().getLogfileName(),
                        entry.getHeader().getLogfileOffset(), dataBase,
                        tableName, eventType,
                        executeTime, DateUtil.format(executeTime),
                        entry.getHeader().getGtid(), delayTime);

                String tableUnion = String.format(ProjectConstant.URL_DATABASE_TABLE_FORMAT, address, dataBase, tableName);
                Set<Integer> jobIds  = IncrementUtil.getTableJobsMap().get(tableUnion);
                if (CollectionUtils.isEmpty(jobIds)) {
                    log.warn("dataBase:{}, tableName:{}, jobIds is empty", dataBase, tableName);
                    continue;
                }
                // mysql
                printXAInfo(rowChange.getPropsList());
                List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
                for (CanalEntry.RowData rowData : rowDataList) {
                    boolean flag = StringUtils.isNotBlank(sql) && (sql.startsWith("ALTER TABLE") || sql.startsWith("CREATE TABLE"));
                    if (flag) {
                        executeSql(jobIds, sql, dataBase, tableName);
                        continue;
                    }
                    if (eventType == EventType.DELETE) {
                        printColumn(rowData.getBeforeColumnsList());
                        deleteColumn(jobIds, rowData.getBeforeColumnsList(), executeTime, dataBase, tableName);
                    } else if (eventType == CanalEntry.EventType.INSERT) {
                        printColumn(rowData.getAfterColumnsList());
                        insertColumn(jobIds, rowData.getAfterColumnsList(), executeTime, dataBase, tableName);
                    } else {
                        printColumn(rowData.getAfterColumnsList());
                        updateColumn(jobIds, rowData.getAfterColumnsList(), executeTime, dataBase, tableName);
                    }
                }
            }
        }
    }

    private void printXAInfo(List<CanalEntry.Pair> pairs) {
        if (pairs == null) {
            return;
        }

        String xaType = null;
        String xaXid = null;
        for (CanalEntry.Pair pair : pairs) {
            String key = pair.getKey();
            if (StringUtils.endsWithIgnoreCase(key, "XA_TYPE")) {
                xaType = pair.getValue();
            } else if (StringUtils.endsWithIgnoreCase(key, "XA_XID")) {
                xaXid = pair.getValue();
            }
        }

        if (xaType != null && xaXid != null) {
            log.info(" ------> " + xaType + " " + xaXid);
        }
    }

    /**
     * 执行sql语句
     * @param jobIds
     * @param sql
     * @param dataBase
     * @param tableName
     */
    private void executeSql(Set<Integer> jobIds, String sql, String dataBase, String tableName) {
        for (Integer jobId : jobIds) {
            ConvertInfo convertInfo = IncrementUtil.getConvertInfo(jobId);
            if (convertInfo == null) {
                log.error("jobId:{}, 不存在读写装换关系", jobId);
                continue;
            }
            //清除预置语句
            convertInfo.setInsertSql(null);
            convertInfo.setConditionSql(null);
            convertInfo.setDeleteSql(null);
            //执行ddl语句
            DataSource dataSource = MysqlUtil.getDataSource(convertInfo.getWriteUrl(), jobId);
            if (dataSource==null) {
                log.error("jobId:{}, 无法找到可用数据源", convertInfo.getJobId());
                continue;
            }
            String toDataBase = convertInfo.getWriteUrl().substring(convertInfo.getWriteUrl().lastIndexOf("/")+1);
            String tempSql = sql.replace(dataBase, toDataBase).replace(tableName, convertInfo.getTableName());
            Connection connection = null;
            try {
                connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                boolean execute = statement.execute(tempSql);
                log.info("sql:{} execute result:{}", tempSql, execute);
            } catch (Exception e) {
                log.error("execute error:", e);
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

    /**
     * 打印行记录
     * @param columns
     */
    private void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            StringBuilder builder = new StringBuilder();
            if (StringUtils.containsIgnoreCase(column.getMysqlType(), "BLOB")
                    || StringUtils.containsIgnoreCase(column.getMysqlType(), "BINARY")) {
                // get value bytes
                builder.append(column.getName()).append(" : ")
                        .append(new String(column.getValue().getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));
            } else {
                builder.append(column.getName()).append(" : ").append(column.getValue());
            }
            builder.append("    type=").append(column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=").append(column.getUpdated());
            }
            log.debug(builder.toString());
        }
    }

    /**
     * 插入
     * @param jobIds
     * @param columns
     * @param executeTime
     * @param dataBase
     * @param tableName
     */
    private void insertColumn(Set<Integer> jobIds, List<CanalEntry.Column> columns, long executeTime, String dataBase, String tableName) {
        Map<String, ColumnValue> insertColumns = Maps.newHashMapWithExpectedSize(columns.size());
        String name;
        ColumnValue columnValue;
        String idValue = null;
        List<String> idList = new ArrayList<>();
        for (CanalEntry.Column column : columns) {
            name = column.getName();
            if(ProjectConstant.DEFAULT_MYSQL_KEY.equals(name)) {
                idValue = column.getValue();
            }
            columnValue = new ColumnValue(column);
            insertColumns.put(name,columnValue);
            if(column.getIsKey()) {
                idList.add(column.getValue());
            }
        }
        if (!idList.isEmpty()) {
            idValue = String.join("_", idList);
        } else if (StringUtils.isBlank(idValue)) {
            log.error("dataBase:{}, table:{},主键不明", dataBase, tableName);
            return;
        }

        for (Integer jobId : jobIds) {
            ConvertInfo convertInfo = IncrementUtil.isContinue(jobId, CanalEntry.EventType.INSERT.name(), ProjectConstant.INCREMENT_SYNC_TYPE.CANAL, executeTime, insertColumns, null, idValue);
            if (convertInfo==null) {
                continue;
            }
            try {
                insert(convertInfo, insertColumns);
            } catch (Exception e) {
                IncrementUtil.saveWaiting(jobId, CanalEntry.EventType.INSERT.name(), ProjectConstant.INCREMENT_SYNC_TYPE.CANAL, insertColumns, null, idValue);
            }
        }
    }


    /**
     * 更新
     * @param jobIds
     * @param columns
     * @param executeTime
     * @param dataBase
     * @param tableName
     */
    private void updateColumn(Set<Integer> jobIds, List<CanalEntry.Column> columns, long executeTime, String dataBase, String tableName) {
        Map<String, ColumnValue> updateColumns = Maps.newHashMapWithExpectedSize(columns.size());
        Map<String, ColumnValue> conditionColumns = Maps.newHashMapWithExpectedSize(columns.size());
        String name;
        ColumnValue columnValue;
        String idValue = null;
        Map<String, ColumnValue> idColumns = Maps.newHashMapWithExpectedSize(1);
        List<String> idList = new ArrayList<>();
        for (CanalEntry.Column column : columns) {
            name = column.getName();
            if(ProjectConstant.DEFAULT_MYSQL_KEY.equalsIgnoreCase(name)) {
                idValue = column.getValue();
                columnValue = new ColumnValue(column);
                idColumns.put(name,columnValue);
            }
            if(column.getIsKey()) {
                idList.add(column.getValue());
                columnValue = new ColumnValue(column);
                conditionColumns.put(name,columnValue);
            }
            if (column.getUpdated()) {
                columnValue = new ColumnValue(column);
                updateColumns.put(name,columnValue);
            }
        }
        if (!idList.isEmpty()) {
            idValue = String.join("_", idList);
        } else if (StringUtils.isBlank(idValue)) {
            log.error("dataBase:{}, table:{},主键不明", dataBase, tableName);
            return;
        } else {
            conditionColumns = idColumns;
        }
        for (Integer jobId : jobIds) {
            ConvertInfo convertInfo = IncrementUtil.isContinue(jobId, EventType.UPDATE.name(), ProjectConstant.INCREMENT_SYNC_TYPE.CANAL, executeTime, updateColumns, conditionColumns, idValue);
            if (convertInfo==null) {
                continue;
            }
            try {
                update(convertInfo, updateColumns, conditionColumns);
            } catch (Exception e) {
                IncrementUtil.saveWaiting(jobId, EventType.UPDATE.name(), ProjectConstant.INCREMENT_SYNC_TYPE.CANAL, updateColumns, conditionColumns, idValue);
            }
        }
    }


    /**
     * 删除
     * @param jobIds
     * @param columns
     * @param executeTime
     * @param dataBase
     * @param tableName
     */
    private void deleteColumn(Set<Integer> jobIds, List<CanalEntry.Column> columns, long executeTime, String dataBase, String tableName) {
        Map<String, ColumnValue> conditionColumns = Maps.newHashMapWithExpectedSize(columns.size());
        String name;
        ColumnValue columnValue;
        String idValue = null;
        List<String> idList = new ArrayList<>();
        Map<String, ColumnValue> idColumns = Maps.newHashMapWithExpectedSize(1);
        for (CanalEntry.Column column : columns) {
            name = column.getName();
            if(ProjectConstant.DEFAULT_MYSQL_KEY.equalsIgnoreCase(name)) {
                idValue = column.getValue();
                columnValue = new ColumnValue(column);
                idColumns.put(name,columnValue);
            }
            if(column.getIsKey()) {
                idList.add(column.getValue());
                columnValue = new ColumnValue(column);
                conditionColumns.put(name,columnValue);
            }
        }
        if (!idList.isEmpty()) {
            idValue = String.join("_", idList);
        } else if (StringUtils.isBlank(idValue)) {
            log.error("dataBase:{}, table:{},主键不明", dataBase, tableName);
            return;
        } else {
            conditionColumns = idColumns;
        }
        for (Integer jobId : jobIds) {
            ConvertInfo convertInfo = IncrementUtil.isContinue(jobId, EventType.DELETE.name(), ProjectConstant.INCREMENT_SYNC_TYPE.CANAL, executeTime, null, conditionColumns, idValue);
            if (convertInfo == null) {
                continue;
            }
            try {
                delete(convertInfo, conditionColumns);
            } catch (Exception e) {
                IncrementUtil.saveWaiting(jobId, EventType.DELETE.name(), ProjectConstant.INCREMENT_SYNC_TYPE.CANAL, null, conditionColumns, idValue);
            }
        }
    }

    /**
     * 插入
     * @param convertInfo
     * @param data
     */
    public static void insert(ConvertInfo convertInfo, Map<String, ColumnValue> data) throws SQLException {
        DataSource dataSource = MysqlUtil.getDataSource(convertInfo.getWriteUrl(), convertInfo.getJobId());
        if (dataSource==null) {
            log.error("jobId:{}, 无法找到可用数据源", convertInfo.getJobId());
            return;
        }
        // 数据源中的各个表
        Map<String, ConvertInfo.ToColumn> relation = convertInfo.getTableColumns();
        Map<String, ColumnValue> mysqlData = columnConvert(data, relation);
        // 每个表 执行sql
        String insertSql = MysqlUtil.doGetInsertSql(convertInfo, mysqlData.keySet());
        int update;
        int index = 0;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
            List<String> valueParam = new ArrayList<>();
            setValues(index, preparedStatement, mysqlData, valueParam);
            log.info("insertSql:{}, value:{}", insertSql, valueParam);
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
        log.info("statement.executeInsert result:{}", update);
    }

    /**
     * 列装换
     * @param data
     * @param relation
     * @return
     */
    private static Map<String, ColumnValue> columnConvert(Map<String, ColumnValue> data, Map<String, ConvertInfo.ToColumn> relation) {
        // 这里使用 TreeMap
        TreeMap<String, ColumnValue> mysqlData = new TreeMap<>();
        for (Map.Entry<String, ConvertInfo.ToColumn> entry : relation.entrySet()) {
            String fromColumn = entry.getKey();
            String toColumn = entry.getValue().getName();
            if (data.containsKey(fromColumn)) {
                mysqlData.put(toColumn, data.get(fromColumn));
            }
        }
        return mysqlData;
    }

    /**
     * 设置值
     * @param index
     * @param preparedStatement
     * @param mysqlData
     * @param valueParam
     * @throws SQLException
     */
    private static int setValues(int index, PreparedStatement preparedStatement, Map<String, ColumnValue> mysqlData, List<String> valueParam) throws SQLException {
        ColumnValue columnValue;
        String value;
        for (Map.Entry<String, ColumnValue> entry : mysqlData.entrySet()) {
            index ++;
            columnValue = entry.getValue();
            value = columnValue.getValue();
            valueParam.add(value);
            if (value==null) {
                preparedStatement.setNull(index, Types.OTHER);
                continue;
            }
            int sqlType = columnValue.getSqlType();
            if (Types.NUMERIC == sqlType || Types.DECIMAL == sqlType) {
                preparedStatement.setObject(index, value, columnValue.getSqlType(), columnValue.getScale());
            } else {
                preparedStatement.setObject(index, value, columnValue.getSqlType());
            }
        }
        return index;
    }

    /**
     * 更新
     * @param convertInfo
     * @param updateData
     * @param conditionData
     */
    public static void update(ConvertInfo convertInfo, Map<String, ColumnValue> updateData, Map<String, ColumnValue> conditionData) throws SQLException {
        // 数据源中的各个表
        Map<String, ConvertInfo.ToColumn> relation = convertInfo.getTableColumns();

        Map<String, ColumnValue> covertUpdateData = columnConvert(updateData, relation);
        Map<String, ColumnValue> covertConditionData = columnConvert(conditionData, relation);
        if (covertUpdateData.isEmpty() || covertConditionData.isEmpty()) {
            log.info("无需更新");
            return;
        }
        DataSource dataSource = MysqlUtil.getDataSource(convertInfo.getWriteUrl(), convertInfo.getJobId());
        if (dataSource==null) {
            log.error("jobId:{}, 无法找到可用数据源", convertInfo.getJobId());
            return;
        }
        // 每个表 执行sql
        String conditionSql = MysqlUtil.doGetConditionSql(convertInfo, covertConditionData.keySet());
        String updateSql = MysqlUtil.doGetUpdateSql(convertInfo.getTableName(), covertUpdateData.keySet(), conditionSql);
        int update;
        int index = 0;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(updateSql);
            List<String> valueParam = new ArrayList<>();
            index = setValues(index, preparedStatement, covertUpdateData, valueParam);
            List<String> condition = new ArrayList<>();
            setValues(index, preparedStatement, covertConditionData, condition);
            log.info("updateSql:{}, value:{}, condition:{}", updateSql, valueParam, condition);
            update = preparedStatement.executeUpdate();
        } catch (Exception e) {
            log.error("columns.size:{} index:{}, error: ", covertUpdateData.size(), index, e);
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
     * @param conditionData
     */
    public static void delete(ConvertInfo convertInfo, Map<String, ColumnValue> conditionData) throws SQLException {
        // 数据源中的各个表
        Map<String, ConvertInfo.ToColumn> relation = convertInfo.getTableColumns();
        Map<String, ColumnValue> covertConditionData = columnConvert(conditionData, relation);
        if (covertConditionData.isEmpty()) {
            log.info("没有主键进行删除");
            return;
        }
        DataSource dataSource = MysqlUtil.getDataSource(convertInfo.getWriteUrl(), convertInfo.getJobId());
        if (dataSource==null) {
            log.error("jobId:{}, 无法找到可用数据源", convertInfo.getJobId());
            return;
        }
        // 每个表 执行sql
        String deleteSql = MysqlUtil.doGetDeleteSql(convertInfo, covertConditionData.keySet());
        int index = 0;
        Connection connection = null;
        List<String> condition = new ArrayList<>();
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(deleteSql);
            setValues(index, preparedStatement, covertConditionData, condition);
            log.info("deleteSql:{}, condition:{}", deleteSql, condition);
            int update = preparedStatement.executeUpdate();
            log.info("affected row:{}", update);
        } catch (Exception e) {
            log.error("condition:{}, delete error:{}", condition, e);
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
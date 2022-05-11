package com.wugui.datax.admin.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wenwo.cloud.message.driven.producer.service.MessageProducerService;
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
import java.util.concurrent.ConcurrentHashMap;
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

    private final Map<Integer, List<CanalJobInfo>> jobs = new ConcurrentHashMap<>();

    private final MessageProducerService messageProducerService;

    public void noRun() {
        this.running = false;
    }

    /**
     * 构造函数
     * @param address
     */
    public CanalWorkThread(String address, MessageProducerService messageProducerService) {
        this.address = address;
        this.messageProducerService = messageProducerService;
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
                log.error("process error:", e);
                if (e.getCause() instanceof InterruptedException) {
                    return;
                }
                // 处理失败, 回滚数据
                try {
                    if (connector.checkValid()) {
                        connector.rollback();
                    }
                } catch (Throwable canalClientException) {
                    log.error("CanalClientException:", e);
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

        log.debug(CONTEXT_FORMAT, batchId, size, memsize, DateUtil.format(), startPosition, endPosition);
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
                    log.debug(" BEGIN ----> Thread id: {}", begin.getThreadId());
                    printXAInfo(begin.getPropsList());
                } else {
                    TransactionEnd end;
                    try {
                        end = TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    log.debug(" END ----> transaction id: {}", end.getTransactionId());
                    printXAInfo(end.getPropsList());
                    log.debug(TRANSACTION_FORMAT,
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
                Map<String, Set<Integer>> tableJobsMap = IncrementUtil.getTableJobsMap();
                Set<Integer> jobIds  = tableJobsMap.get(tableUnion);
                if (CollectionUtils.isEmpty(jobIds)) {
                    jobIds  = tableJobsMap.get(tableUnion.replace("_3306", ""));
                    if (CollectionUtils.isEmpty(jobIds)) {
                        log.warn("dataBase:{}, tableName:{}, jobIds is empty", dataBase, tableName);
                        continue;
                    }
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

        //异步处理各个任务
        if (!jobs.isEmpty()) {
            jobs.forEach((k,v) -> {
                if (!v.isEmpty()) {
                    messageProducerService.sendMsg(v, k.toString());
                }
            });
            jobs.clear();
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
            List<CanalJobInfo> canalJobInfos = getCanalJobInfos(jobId);
            CanalJobInfo canalJobInfo = CanalJobInfo.builder().eventType(EventType.INSERT).updateColumns(insertColumns).idValue(idValue).build();
            canalJobInfos.add(canalJobInfo);
        }
    }

    /**
     * 获取记录
     * @param jobId
     * @return
     */
    private List<CanalJobInfo> getCanalJobInfos(Integer jobId) {
        List<CanalJobInfo> canalJobInfos;
        if (jobs.containsKey(jobId)) {
            canalJobInfos = jobs.get(jobId);
        } else {
            canalJobInfos = new ArrayList<>();
            jobs.put(jobId, canalJobInfos);
        }
        return canalJobInfos;
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
            List<CanalJobInfo> canalJobInfos = getCanalJobInfos(jobId);
            CanalJobInfo canalJobInfo = CanalJobInfo.builder().eventType(EventType.UPDATE).updateColumns(updateColumns).conditionColumns(conditionColumns).idValue(idValue).build();
            canalJobInfos.add(canalJobInfo);
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
            List<CanalJobInfo> canalJobInfos = getCanalJobInfos(jobId);
            CanalJobInfo canalJobInfo = CanalJobInfo.builder().eventType(EventType.DELETE).conditionColumns(conditionColumns).idValue(idValue).build();
            canalJobInfos.add(canalJobInfo);
        }
    }
}
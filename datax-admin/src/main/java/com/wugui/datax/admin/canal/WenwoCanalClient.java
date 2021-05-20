package com.wugui.datax.admin.canal;

import cn.hutool.core.collection.CollectionUtil;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.Pair;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionBegin;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionEnd;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.core.trigger.JobTrigger;
import com.wugui.datax.admin.entity.JobInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Doctor
 * 提供配合 datax存量同步的 增量消费binlog能力
 *
 */
@Component
public class WenwoCanalClient implements SmartInitializingSingleton {

    private final static Logger logger             = LoggerFactory.getLogger(WenwoCanalClient.class);
    private static final String             SEP                = SystemUtils.LINE_SEPARATOR;
    private static final String             DATE_FORMAT        = "yyyy-MM-dd HH:mm:ss";
    private static volatile boolean                running            = false;

    @Value("${destination:example}")
    private String destination;

    @Value("${canal.server.address:10.200.0.225}")
    private String canalServerAddress;

    @Value("${canal.server.port:11111}")
    private int canalServerPort;

    @Value("${canal.username:canal}")
    private String canalUsername;

    @Value("${canal.password:canal}")
    private String canalPassword;

    @Value("${canal.pull.interval:1000}")
    private Integer interval;

    private CanalConnector connector;

    private static String                   context_format;
    private static String                   row_format;
    private static String                   transaction_format;

    static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************" + SEP;

        row_format = SEP
                     + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {}({}) , gtid : ({}) , delay : {} ms"
                     + SEP;

        transaction_format = SEP
                             + "================> binlog[{}:{}] , executeTime : {}({}) , gtid : ({}) , delay : {}ms"
                             + SEP;
    }

    @Override
    public void afterSingletonsInstantiated() {
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalServerAddress, canalServerPort),
                    destination, canalUsername, canalPassword);
        ThreadPoolExecutor canalWorkerThead = new ThreadPoolExecutor(1, 1, 60 * 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
            Thread thread = new Thread(r);
            thread.setName("canalWorkerThead");
            return thread;
        }, new ThreadPoolExecutor.CallerRunsPolicy());

        // 初始化原有的canalJobs
        List<JobInfo> initCanalJobs = JobAdminConfig.getAdminConfig().getJobInfoMapper().findInitCanal();
        if (CollectionUtil.isNotEmpty(initCanalJobs)) {
            for (JobInfo initCanalJob : initCanalJobs) {
                JobTrigger.initCanal(initCanalJob);
            }
        }

        // 触发同步任务
        canalWorkerThead.execute(this::process);
        running = true;
    }

    private void process() {
        int round = 0;
        int batchSize = 5 * 1024;
        while (running) {
            try {
                MDC.put("destination", destination);
                connector.connect();
                logger.info("connect to canalServerAddress:{}", canalServerAddress);
                connector.subscribe();
                while (running) {
                    // 获取指定数量的数据
                    Message message = connector.getWithoutAck(batchSize);
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        try {
                            Thread.sleep(interval);
                            if (round++ % 300 == 0) {
                                logger.info("sleeping--------");
                            }
                        } catch (InterruptedException e) {
                            logger.error("InterruptedException", e);
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
            } catch (Throwable e) {
                logger.error("process error!", e);
                try {
                    if (DataSourceFactory.instance().isEmpty()) {
                        Thread.sleep(10000L);
                    } else {
                        Thread.sleep(1000L);
                    }
                } catch (InterruptedException e1) {
                    logger.error("InterruptedException", e1);
                }
                // 处理失败, 回滚数据
                try {
                connector.rollback();
                } catch (Throwable canalClientException) {
                    logger.error("process error!", e);
                }
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    private void printSummary(Message message, long batchId, int size) {
        long memsize = 0;
        for (Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format, batchId, size, memsize, format.format(new Date()), startPosition, endPosition);
    }

    private String buildPositionForDump(Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        String position = entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
                + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
        if (StringUtils.isNotEmpty(entry.getHeader().getGtid())) {
            position += " gtid(" + entry.getHeader().getGtid() + ")";
        }
        return position;
    }

    private void printEntry(List<Entry> entrys) throws SQLException {
        for (Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = System.currentTimeMillis() - executeTime;
            Date date = new Date(entry.getHeader().getExecuteTime());
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                    TransactionBegin begin;
                    try {
                        begin = TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    logger.info(transaction_format,
                            entry.getHeader().getLogfileName(),
                            entry.getHeader().getLogfileOffset(),
                            entry.getHeader().getExecuteTime(), simpleDateFormat.format(date),
                            entry.getHeader().getGtid(), delayTime);
                    logger.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
                    printXAInfo(begin.getPropsList());
                } else {
                    TransactionEnd end;
                    try {
                        end = TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    logger.info(" END ----> transaction id: {}", end.getTransactionId());
                    printXAInfo(end.getPropsList());
                    logger.info(transaction_format,
                            entry.getHeader().getLogfileName(),
                            entry.getHeader().getLogfileOffset(),
                            entry.getHeader().getExecuteTime(), simpleDateFormat.format(date),
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
                EventType eventType = rowChange.getEventType();

                String tableName = entry.getHeader().getTableName();
                String dataBase = entry.getHeader().getSchemaName();
                logger.info(row_format,
                        entry.getHeader().getLogfileName(),
                        entry.getHeader().getLogfileOffset(), dataBase,
                        tableName, eventType,
                        entry.getHeader().getExecuteTime(), simpleDateFormat.format(date),
                        entry.getHeader().getGtid(), delayTime);

                if (eventType == EventType.QUERY || rowChange.getIsDdl()) {
                    String sql = rowChange.getSql();
                    logger.info(" sql ----> " + sql + SEP);
                    // 特殊处理
                    if (sql.contains("medic_courses_info") || sql.contains("medic_series_info")) {
                        try {
                            fixData(sql);
                        } catch (RuntimeException e) {
                            logger.error("RuntimeException current sql:{}", sql, e);
                        }
                    }
                    continue;
                }

                // mysql
                printXAInfo(rowChange.getPropsList());
                List<RowData> rowDatasList = rowChange.getRowDatasList();
                // 数据来源
                String dataBaseTable = dataBase + "__" + tableName;
                DataSource dataSource = DataSourceFactory.instance().getDataSource(dataBaseTable);
                if (dataSource == null) {
                    logger.info("dataBase{}, tableName{} 没有配置同步数据源", dataBase, tableName);
                    return;
                }
                // 数据去向
                tableName = DataSourceFactory.instance().convertTableName(tableName);
                Long initTimestamp = DataSourceFactory.instance().getInitTimestamp(dataBaseTable);
                if (executeTime < initTimestamp) {
                    continue;
                }
                for (RowData rowData : rowDatasList) {
                    if (eventType == EventType.DELETE) {
                        printColumn(rowData.getBeforeColumnsList());
                        deleteColumn(rowData.getBeforeColumnsList(), tableName, dataSource);
                    } else if (eventType == EventType.INSERT) {
                        printColumn(rowData.getAfterColumnsList());
                        insertColumn(rowData.getAfterColumnsList(), tableName, dataSource);
                    } else {
                        printColumn(rowData.getAfterColumnsList());
                        updateColumn(rowData.getAfterColumnsList(), tableName, dataSource);
                    }
                }
            }
        }
    }

    private void fixData(String sql) {

    }

    private void printColumn(List<Column> columns) {
        for (Column column : columns) {
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
            logger.info(builder.toString());
        }
    }

    private void printXAInfo(List<Pair> pairs) {
        if (pairs == null) {
            return;
        }

        String xaType = null;
        String xaXid = null;
        for (Pair pair : pairs) {
            String key = pair.getKey();
            if (StringUtils.endsWithIgnoreCase(key, "XA_TYPE")) {
                xaType = pair.getValue();
            } else if (StringUtils.endsWithIgnoreCase(key, "XA_XID")) {
                xaXid = pair.getValue();
            }
        }

        if (xaType != null && xaXid != null) {
            logger.info(" ------> " + xaType + " " + xaXid);
        }
    }

    /**
     * 插入
     *
     * @param columns 列
     * @param tableName 表名
     * @param dataSource
     */
    private void insertColumn(List<Column> columns, String tableName, DataSource dataSource) {
        StringBuilder insertBuilder = new StringBuilder("INSERT INTO `" + tableName + "` (");
        StringBuilder values = new StringBuilder("VALUES(");
        for (Column column : columns) {
            // 字段是否存在映射关系
            String name = column.getName();
            insertBuilder.append("`").append(name).append("`").append(",");
            // 值
            values.append("?").append(",");
        }
        if (values.length() == "VALUES(".length()) {
            logger.info("没有指定的值插入");
            return;
        }
        values.delete(values.length() - 1, values.length()).append(");");
        insertBuilder.delete(insertBuilder.length() - 1, insertBuilder.length()).append(")").append(values.toString());
        String insertSql = insertBuilder.toString();
        logger.info("prepareStatement insertSql:{}", insertSql);
        int update = 0;
        int index = 0;

        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
            String value;
            List<String> valueParam = new ArrayList<>();
            for (Column column : columns) {
                index ++;
                value = column.getValue();
                valueParam.add(value);
                if (column.getIsNull()) {
                    preparedStatement.setNull(index, Types.OTHER);
                    continue;
                }
                preparedStatement.setObject(index, value, column.getSqlType());
            }
            logger.info("insertSql:{}, value:{}", insertSql, valueParam);
            update = preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("error,columns.size:{} index:{} ", columns.size(), index, e);
        } finally {
            try {
                assert connection != null;
                // 回收,并非关闭
                connection.close();
            } catch (SQLException e) {
                logger.error("close Exception", e);
            }
        }
        logger.info("statement.executeUpdate result:{}", update);
    }


    /**
     * 删除
     *
     * @param columns 列
     * @param tableName  表名
     * @param dataSource
     */
    private void deleteColumn(List<Column> columns, String tableName, DataSource dataSource) {
        StringBuilder deleteBuilder = new StringBuilder("DELETE FROM `" + tableName +"` WHERE ");
        // 字段
        int idType = 0;
        String idValue = "";
        for (Column column : columns) {
            String name = column.getName();
            if ("id".equalsIgnoreCase(name)) {
                deleteBuilder.append(name).append("=").append("?");
                idType = column.getSqlType();
                idValue = column.getValue();
                break;
            }
        }
        String deleteSql = deleteBuilder.toString();
        logger.info("deleteSql:{}", deleteSql);
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection. prepareStatement(deleteSql);
            preparedStatement.setObject(1, idValue, idType);
            int update = preparedStatement.executeUpdate();
            if (update == 0) {

            }
            logger.info("affected row:{}", update);
        } catch (Exception e) {
            logger.error("error,columns.size:{}", columns.size(), e);
        } finally {
            try {
                assert connection != null;
                connection.close();
            } catch (SQLException e) {
                logger.error("close Exception", e);
            }
        }
    }

    private final BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>(2000);

    private final ThreadPoolExecutor updateExecutor = new ThreadPoolExecutor(8, 16,
            10 * 60,TimeUnit.SECONDS, taskQueue, new ThreadPoolExecutor.CallerRunsPolicy());

    /**
     * 按表名和id 为key 的顺序队列
     */
    private final ConcurrentMap<String, LinkedBlockingQueue<UpdateWorker>> updateByOrderQueue = new ConcurrentHashMap<>();

    /**
     * 包含key代表在运行中
     */
    private final ConcurrentMap<String, Boolean> runningStatus = new ConcurrentHashMap<>();

    /**
     * 更新
     *
     * @param columns 列
     * @param tableName 表名
     * @param dataSource
     */
    private void updateColumn(List<Column> columns, String tableName, DataSource dataSource) throws SQLException {
        StringBuilder updateBuilder = new StringBuilder("UPDATE `" + tableName + "` SET");
        StringBuilder whereClause = new StringBuilder(" WHERE ");
        // 字段
        int idType = 0;
        String idValue = "";
        for (Column column : columns) {
            String name = column.getName();
            if ("id".equalsIgnoreCase(name)) {
                whereClause.append(name).append("=").append("?");
                idType = column.getSqlType();
                idValue = column.getValue();
            }
            if (column.getUpdated()) {
                updateBuilder.append("`").append(name).append("`")
                        .append("=").append("?").append(",");
            }
        }

        updateBuilder.delete(updateBuilder.length() - 1, updateBuilder.length()).append(whereClause.toString());
        String updateSql = updateBuilder.toString();

        int updateCount = execUpdate(columns, dataSource, idType, idValue, updateSql);
        // updateCount为0说明id不存在,进入按表和id为key的顺序任务队列
        if (updateCount == 0) {
            // 构建任务 同一个表的同一条数据需要 进入同一队列里面顺序执行
            UpdateWorker updateWorker = new UpdateWorker(columns, dataSource, idType, idValue, tableName, updateSql);
            String taskKey = tableName + idValue;
            LinkedBlockingQueue<UpdateWorker> queue;
            if (updateByOrderQueue.containsKey(taskKey)) {
                queue = updateByOrderQueue.get(taskKey);
            } else {
                queue = new LinkedBlockingQueue<>(1000);
                updateByOrderQueue.put(taskKey, queue);
            }
            queue.add(updateWorker);

            if (!runningStatus.containsKey(taskKey)) {
                // 没有在运行中, 重新激活任务
                updateExecutor.submit(updateWorker);
            }
            // 已经在运行
        }
        logger.info("updateBuilder sql:{} updateBuilder result:{}\n", updateBuilder.toString(), updateCount);
    }

    private class UpdateWorker implements Runnable{
        private List<Column> columns;
        private DataSource dataSource;
        private int idType;
        private String idValue;
        private String tableName;
        private String updateSql;

        public UpdateWorker(List<Column> columns, DataSource dataSource, int idType,
                            String idValue, String tableName, String updateSql) {
            this.columns = columns;
            this.dataSource = dataSource;
            this.idType = idType;
            this.idValue = idValue;
            this.tableName = tableName;
            this.updateSql = updateSql;
        }

        public void copyData(UpdateWorker from) {
            this.columns = from.columns;
            this.dataSource = from.dataSource;
            this.idType = from.idType;
            this.idValue = from.idValue;
            this.tableName = from.tableName;
            this.updateSql = from.updateSql;
        }

        @Override
        public void run() {
            String taskKey = tableName + idValue;
            runningStatus.put(taskKey, null);
            // 同一个表的同一个数据 需要按顺序更新
            LinkedBlockingQueue<UpdateWorker> updateWorkers = updateByOrderQueue.get(taskKey);
            UpdateWorker poll = updateWorkers.poll();
            while (poll != null) {
                int updateResult = 0;
                while (updateResult == 0) {
                    try {
                        updateResult = execUpdate(columns, dataSource, idType, idValue, updateSql);
                    } catch (SQLException e) {
                        logger.error("updateException", e);
                    }
                    // 更新成功
                    if (updateResult == 1) {
                        break;
                    } else {
                        // 更新失败 id不存在
                        try {
                            logger.info("sleeping for tableName:{},idValue:{}", tableName, idValue);
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            logger.error("close Exception", e);
                        }
                    }
                }
                // 下一个任务(如果有)
                poll = updateWorkers.poll();
                if (poll != null) {
                    copyData(poll);
                }
            }
            runningStatus.remove(taskKey);
        }
    }

    /**
     * 执行更新语句
     *
     * @param columns 列内容
     * @param dataSource 数据源
     * @param idType id类型
     * @param idValue id值
     * @param updateSql 更新sql
     * @return
     * @throws SQLException
     */
    private int execUpdate(List<Column> columns, DataSource dataSource, int idType,
                           String idValue, String updateSql) throws SQLException {
        int index = 0;
        int updateCount;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(updateSql);
            String value;
            List<String> values = new ArrayList<>();
            for (Column column : columns) {
                if (column.getUpdated()) {
                    index++;
                    value = column.getValue();
                    values.add(value);
                    if (column.getIsNull()) {
                        preparedStatement.setNull(index, Types.OTHER);
                        continue;
                    }
                    preparedStatement.setObject(index, value, column.getSqlType());
                }
            }
            preparedStatement.setObject(index + 1, idValue, idType);
            values.add(idValue);
            logger.info("updateSql:{}, value:{}", updateSql, values);
            updateCount = preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("error", e);
            throw e;
        } finally {
            try {
                assert connection != null;
                connection.close();
            } catch (SQLException e) {
                logger.error("close Exception", e);
            }
        }
        return updateCount;
    }

}

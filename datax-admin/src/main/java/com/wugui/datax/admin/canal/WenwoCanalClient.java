package com.wugui.datax.admin.canal;

import cn.hutool.core.collection.CollectionUtil;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wugui.datax.admin.core.conf.JobAdminConfig;
import com.wugui.datax.admin.core.util.IncrementUtil;
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
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

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

    @Value("${destination:example-datax}")
    private String destination;

    @Value("${zookeeper.address:}")
    private String zkServers;

    @Value("${canal.server.address:}")
    private String canalServerAddress;

    @Value("${canal.server.port:11111}")
    private int canalServerPort;

    @Value("${canal.username:}")
    private String canalUsername;

    @Value("${canal.password:}")
    private String canalPassword;

    @Value("${canal.pull.interval:1000}")
    private Integer interval;

    /**
     * 连接重试频率
     */
    @Value("${canal.connection.retry:5}")
    private Integer connectionTry;

    private CanalConnector connector;

    private static final String                   CONTEXT_FORMAT;
    private static final String                   ROW_FORMAT;
    private static final String                   TRANSACTION_FORMAT;

    static {
        CONTEXT_FORMAT = SEP + "****************************************************" + SEP
                + "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP
                + "* Start : [{}] " + SEP
                + "* End : [{}] " + SEP
                +"****************************************************" + SEP;

        ROW_FORMAT = SEP
                + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {}({}) , gtid : ({}) , delay : {} ms"
                + SEP;

        TRANSACTION_FORMAT = SEP
                + "================> binlog[{}:{}] , executeTime : {}({}) , gtid : ({}) , delay : {}ms"
                + SEP;
    }

    /**
     * 获取连接
     * @return
     */
    private CanalConnector getCanalConnector() {
        CanalConnector connector = null;
        if (StringUtils.isNotBlank(zkServers)) {
            connector = CanalConnectors.newClusterConnector(zkServers, destination, canalUsername, canalPassword);
        } else if (StringUtils.isNotBlank(canalServerAddress)) {
            connector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalServerAddress, canalServerPort),
                    destination, canalUsername, canalPassword);
        }
        if (connector == null) {
            throw new RuntimeException("canal节点为空");
        }
        return connector;
    }

    @Override
    public void afterSingletonsInstantiated() {
        connector = getCanalConnector();
        ThreadPoolExecutor canalWorkerThead = new ThreadPoolExecutor(1, 1, 60 * 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
            Thread thread = new Thread(r);
            thread.setName("canalWorkerThead");
            return thread;
        }, new ThreadPoolExecutor.CallerRunsPolicy());

        // 初始化原有的canalJobs
        List<JobInfo> initCanalJobs = JobAdminConfig.getAdminConfig().getJobInfoMapper().findInitCanal();
        if (CollectionUtil.isNotEmpty(initCanalJobs)) {
            for (JobInfo initCanalJob : initCanalJobs) {
                IncrementUtil.initCanal(initCanalJob);
            }
        }

        // 触发同步任务
        canalWorkerThead.execute(this::process);
        running = true;
    }

    private void process() {
        int round = 0;
        int batchSize = 5 * 1024;
        int retry = 0;
        long sleepTime = 1000L;
        while (running) {
            try {
                MDC.put("destination", destination);
                connector.connect();
                logger.info("connect to canalServerAddress:{}", StringUtils.isNotBlank(zkServers)?zkServers:canalServerAddress);
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
                        if (retry < connectionTry) {
                            retry ++;
                            sleepTime = sleepTime * retry;
                        }
                        Thread.sleep(sleepTime);
                    }
                } catch (InterruptedException e1) {
                    logger.error("InterruptedException", e1);
                }
                // 处理失败, 回滚数据
                try {
                    if (connector.checkValid()) {
                        connector.rollback();
                    }
                } catch (Throwable canalClientException) {
                    logger.error("process error!", e);
                }
            } finally {
                connector.disconnect();
                MDC.remove("destination");
                connector = getCanalConnector();
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
        logger.info(CONTEXT_FORMAT, batchId, size, memsize, format.format(new Date()), startPosition, endPosition);
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
                    logger.info(TRANSACTION_FORMAT,
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
                    logger.info(TRANSACTION_FORMAT,
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
                // 数据来源
                String dataBaseTable = dataBase + "|" + tableName;
                Long initTimestamp = DataSourceFactory.instance().getInitTimestamp(dataBaseTable);
                if (initTimestamp!=null && executeTime < initTimestamp) {
                    continue;
                }
                String sql = rowChange.getSql();
                if (eventType == EventType.QUERY) {
                    continue;
                }

                logger.info(ROW_FORMAT,
                        entry.getHeader().getLogfileName(),
                        entry.getHeader().getLogfileOffset(), dataBase,
                        tableName, eventType,
                        entry.getHeader().getExecuteTime(), simpleDateFormat.format(date),
                        entry.getHeader().getGtid(), delayTime);

                // mysql
                printXAInfo(rowChange.getPropsList());
                List<RowData> rowDataList = rowChange.getRowDatasList();

                DruidDataSource dataSource = DataSourceFactory.instance().getDataSource(dataBaseTable);
                if (dataSource == null) {
                    logger.info("dataBase:{}, tableName:{} 没有配置同步数据源", dataBase, tableName);
                    continue;
                }
                // 数据去向
                String convertTableName = DataSourceFactory.instance().convertTableName(tableName);
                if (convertTableName==null) {
                    continue;
                }
                for (RowData rowData : rowDataList) {
                    if (StringUtils.isNotBlank(sql) && (sql.startsWith("ALTER TABLE") || sql.startsWith("CREATE TABLE"))) {
                        String url = dataSource.getUrl();
                        url = url.substring(0, url.indexOf("?"));
                        String  toDataBase = url.replaceAll("jdbc:mysql://.*?:.*?/(.*)", "$1");
                        sql = sql.replace(dataBase, toDataBase).replace(tableName, convertTableName);
                        executeSql(dataSource, sql);
                        continue;
                    }

                    Map<String, String> convertTableColumn = DataSourceFactory.instance().convertTableColumn(dataBaseTable);
                    if (CollectionUtils.isEmpty(convertTableColumn)) {
                        continue;
                    }
                    if (eventType == EventType.DELETE) {
                        printColumn(rowData.getBeforeColumnsList());
                        deleteColumn(rowData.getBeforeColumnsList(), convertTableName, convertTableColumn, dataSource);
                    } else if (eventType == EventType.INSERT) {
                        printColumn(rowData.getAfterColumnsList());
                        insertColumn(rowData.getAfterColumnsList(), convertTableName, convertTableColumn, dataSource);
                    } else {
                        printColumn(rowData.getAfterColumnsList());
                        updateColumn(rowData.getAfterColumnsList(), convertTableName, convertTableColumn, dataSource);
                    }
                }
            }
        }
    }


    /**
     * 执行sql语句
     * @param dataSource
     * @param sql
     */
    private void executeSql(DataSource dataSource, String sql) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            boolean execute = statement.execute(sql);
            logger.info("sql:{} execute result:{}", sql, execute);
        } catch (Exception e) {
            logger.error("execute error:", e);
        } finally {
            try {
                assert connection != null;
                connection.close();
            } catch (SQLException e) {
                logger.error("close Exception", e);
            }
        }
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
     * @param columnRelation
     * @param dataSource
     */
    private void insertColumn(List<Column> columns, String tableName, Map<String,String> columnRelation, DataSource dataSource) {
        StringBuilder insertBuilder = new StringBuilder("INSERT INTO `" + tableName + "` (");
        StringBuilder values = new StringBuilder(" VALUES(");
        List<CanalEntry.Column> insertColumns = new ArrayList<>();
        for (Column column : columns) {
            // 字段是否存在映射关系
            String name = column.getName();
            name = columnRelation.get(name);
            if(StringUtils.isNotBlank(name)) {
                insertBuilder.append("`").append(name).append("`").append(",");
                // 值
                values.append("?").append(",");
                insertColumns.add(column);
            }
        }
        if (insertColumns.isEmpty()) {
            logger.info("没有指定的值插入");
            return;
        }
        values.delete(values.length() - 1, values.length()).append(");");
        insertBuilder.delete(insertBuilder.length() - 1, insertBuilder.length()).append(")").append(values.toString());
        String insertSql = insertBuilder.toString();
        logger.info("prepareStatement insertSql:{}", insertSql);

        int index = 0;

        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
            List<String> valueParam = new ArrayList<>();
            index = setValue(preparedStatement, index, insertColumns, valueParam);
            logger.info("insertSql:{}, value:{}", insertSql, valueParam);
            int update = preparedStatement.executeUpdate();
            logger.info("statement.executeUpdate result:{}", update);
        } catch (Exception e) {
            logger.error("error,columns.size:{} index:{} ", insertColumns.size(), index, e);
        } finally {
            try {
                assert connection != null;
                // 回收,并非关闭
                connection.close();
            } catch (SQLException e) {
                logger.error("close Exception", e);
            }
        }
    }

    /**
     * 预编译sql设置值
     * @param preparedStatement
     * @param index
     * @param columns
     * @param valueParam
     * @return
     * @throws SQLException
     */
    private int setValue(PreparedStatement preparedStatement, int index, List<CanalEntry.Column> columns, List<String> valueParam) throws SQLException {
        String value;
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
        return index;
    }


    /**
     * 删除
     *
     * @param columns 列
     * @param tableName  表名
     * @param dataSource
     */
    private void deleteColumn(List<Column> columns, String tableName, Map<String,String> columnRelation, DataSource dataSource) {
        StringBuilder deleteBuilder = new StringBuilder("DELETE FROM `" + tableName +"` WHERE ");
        // 字段
        List<CanalEntry.Column> keyColumns = new ArrayList<>();
        for (Column column : columns) {
            if(column.getIsKey()) {
                // 字段是否存在映射关系
                String name = column.getName();
                name = columnRelation.get(name);
                if (StringUtils.isNotBlank(name)) {
                    deleteBuilder.append("`").append(name).append("`").append("=").append("?").append(" AND");
                    // 值
                    keyColumns.add(column);
                } else {
                    logger.info("主键不对应，无法删除");
                    return;
                }
            }
        }
        if (keyColumns.isEmpty()) {
            logger.info("没有主键进行删除");
            return;
        }
        deleteBuilder.delete(deleteBuilder.length() - 4, deleteBuilder.length());
        String deleteSql = deleteBuilder.toString();

        int index = 0;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection. prepareStatement(deleteSql);
            List<String> valueParam = new ArrayList<>();
            index = setValue(preparedStatement, index, keyColumns, valueParam);
            logger.info("deleteSql:{}, condition:{}", deleteSql, valueParam);
            int update = preparedStatement.executeUpdate();
            logger.info("affected row:{}", update);
            //去除等待的更新语句
            String idValue = keyColumns.stream().map(Column::getValue).collect(Collectors.joining("_"));
            String taskKey = tableName + idValue;
            updateByOrderQueue.remove(taskKey);
        } catch (Exception e) {
            logger.error("error,columns.size:{} index:{} ", keyColumns.size(), index, e);
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
    private void updateColumn(List<Column> columns, String tableName, Map<String,String> columnRelation, DataSource dataSource) throws SQLException {
        StringBuilder updateBuilder = new StringBuilder("UPDATE `" + tableName + "` SET ");
        StringBuilder whereClause = new StringBuilder(" WHERE ");
        // 字段
        List<CanalEntry.Column> keyColumns = new ArrayList<>();
        List<CanalEntry.Column> updateColumns = new ArrayList<>();
        for (Column column : columns) {
            if(column.getIsKey()) {
                // 字段是否存在映射关系
                String name = column.getName();
                name = columnRelation.get(name);
                if (StringUtils.isNotBlank(name)) {
                    whereClause.append("`").append(name).append("`").append("=").append("?").append(" AND");
                    // 值
                    keyColumns.add(column);
                } else {
                    logger.info("主键不对应，无法更新");
                    return;
                }
            }
            if (column.getUpdated()) {
                // 字段是否存在映射关系
                String name = column.getName();
                name = columnRelation.get(name);
                if (StringUtils.isNotBlank(name)) {
                    updateBuilder.append("`").append(name).append("`")
                            .append("=").append("?").append(",");
                    updateColumns.add(column);
                }
            }
        }
        if (keyColumns.isEmpty() || updateColumns.isEmpty()) {
            logger.info("无需更新");
            return;
        }

        whereClause.delete(whereClause.length() - 4, whereClause.length());
        updateBuilder.delete(updateBuilder.length() - 1, updateBuilder.length()).append(whereClause.toString());
        String updateSql = updateBuilder.toString();

        int updateCount = execUpdate(dataSource, updateColumns, keyColumns, updateSql);
        // updateCount为0说明id不存在,进入按表和id为key的顺序任务队列
        if (updateCount == 0) {
            // 构建任务 同一个表的同一条数据需要 进入同一队列里面顺序执行
            String idValue = keyColumns.stream().map(Column::getValue).collect(Collectors.joining("_"));
            UpdateWorker updateWorker = new UpdateWorker(dataSource, idValue, updateColumns, keyColumns, tableName, updateSql);
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
        logger.info("updateBuilder sql:{} updateBuilder result:{}", updateBuilder.toString(), updateCount);
    }

    private class UpdateWorker implements Runnable {
        private String idValue;
        private List<CanalEntry.Column> keyColumns;
        private List<CanalEntry.Column> updateColumns;
        private DataSource dataSource;
        private String tableName;
        private String updateSql;

        public UpdateWorker(DataSource dataSource, String idValue, List<CanalEntry.Column> updateColumns, List<CanalEntry.Column> keyColumns, String tableName, String updateSql) {
            this.dataSource = dataSource;
            this.idValue = idValue;
            this.keyColumns = keyColumns;
            this.updateColumns = updateColumns;
            this.tableName = tableName;
            this.updateSql = updateSql;
        }

        public void copyData(UpdateWorker from) {
            this.dataSource = from.dataSource;
            this.idValue = from.idValue;
            this.keyColumns = from.keyColumns;
            this.updateColumns = from.updateColumns;
            this.tableName = from.tableName;
            this.updateSql = from.updateSql;
        }

        @Override
        public void run() {
            String taskKey = tableName + idValue;
            runningStatus.put(taskKey, null);
            // 同一个表的同一个数据 需要按顺序更新
            LinkedBlockingQueue<UpdateWorker> updateWorkers = updateByOrderQueue.get(taskKey);
            if (!CollectionUtils.isEmpty(updateWorkers)) {
                UpdateWorker poll = updateWorkers.poll();
                while (poll != null) {
                    int updateResult = 0;
                    while (updateResult == 0) {
                        try {
                            updateResult = execUpdate(dataSource, updateColumns, keyColumns, updateSql);
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
            }
            runningStatus.remove(taskKey);
        }
    }

    /**
     * 执行更新语句
     *
     * @param dataSource 数据源
     * @param updateColumns
     * @param keyColumns
     * @param updateSql 更新sql
     * @return
     * @throws SQLException
     */
    private int execUpdate(DataSource dataSource, List<CanalEntry.Column> updateColumns, List<CanalEntry.Column> keyColumns, String updateSql) throws SQLException {
        int index = 0;
        int updateCount;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(updateSql);

            List<String> values = new ArrayList<>();
            index = setValue(preparedStatement, index, updateColumns, values);

            List<String> condition = new ArrayList<>();
            setValue(preparedStatement, index, keyColumns, condition);
            logger.info("updateSql:{}, update:{}, condition:{}", updateSql, values, condition);
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

package com.wugui.datax.admin.canal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.util.IncrementUtil;
import com.wugui.datax.admin.entity.ConvertInfo;
import com.wugui.datax.admin.util.MysqlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;

/**
 * canal异步消费
 * @author chen.ruihong
 * @date 2022/05/07
 */
@Slf4j
public class CanalRabbitListener implements ChannelAwareMessageListener {

	/**
	 * json转换
	 */
	private static final ObjectMapper MAPPER = new ObjectMapper();

	/**
	 * 消息处理
	 * @param message
	 */
	@Override
	public void onMessage(Message message, Channel channel) throws Exception {
		String routingKey = message.getMessageProperties().getReceivedRoutingKey();
		int jobId = Integer.parseInt(routingKey);
		String body = new String(message.getBody());
		Connection connection = null;
		List<CanalJobInfo> canalJobInfos = null;
		List<String> sqlList = new ArrayList<>();
		try {
			canalJobInfos = MAPPER.readValue(body, new TypeReference<List<CanalJobInfo>>(){});
			ConvertInfo convertInfo = IncrementUtil.getConvertInfo(jobId);
			if (canalJobInfos.isEmpty() || convertInfo == null) {
				return;
			}
			DataSource dataSource = MysqlUtil.getDataSource(convertInfo.getWriteUrl(), convertInfo.getJobId());
			if (dataSource == null) {
				log.error("jobId:{}, 无法找到可用数据源", jobId);
				return;
			}
			connection = dataSource.getConnection();
			//手动提交
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();
			String sql;
			for (CanalJobInfo canalJobInfo : canalJobInfos) {
				switch (canalJobInfo.getEventType()) {
					case INSERT:
						sql = doGetInsertSql(convertInfo, canalJobInfo.getUpdateColumns());
						if (StringUtils.isNotBlank(sql)) {
							statement.addBatch(sql);
							sqlList.add(sql);
						}
						break;
					case UPDATE:
						sql = doGetUpdateSql(convertInfo, canalJobInfo.getUpdateColumns(), canalJobInfo.getConditionColumns());
						if (StringUtils.isNotBlank(sql)) {
							statement.addBatch(sql);
							sqlList.add(sql);
						}
						break;
					case DELETE:
						sql = doGetDeleteSql(convertInfo, canalJobInfo.getConditionColumns());
						if (StringUtils.isNotBlank(sql)) {
							statement.addBatch(sql);
							sqlList.add(sql);
						}
						break;
					default:
				}
			}
			int[] results = statement.executeBatch();
			statement.clearBatch();
			statement.close();
			//提交
			connection.commit();
			if (results.length > ProjectConstant.PRINT_EXECUTE_BATCH_SIZE) {
				log.info("jobId:{}, executeBatch size:{}", jobId, results.length);
			}
			channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
			for (int i = 0; i < results.length; i++) {
				log.info("jobId:{}, sql:【{}】, result:{}", jobId, sqlList.get(i), results[i]);
			}
		} catch (JsonProcessingException e) {
			log.error("jobId:{}, str:{}, JsonProcessingException:{}", jobId, body, e.getMessage());
			channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);
		} catch (Exception e) {
			log.error("jobId:{}, Exception:", jobId, e);
			for (String sql : sqlList) {
				log.error("jobId:{}, sql:【{}】", jobId, sql);
			}
			if (connection!=null) {
				connection.rollback();
			}
			channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
			saveFailRecord(jobId, canalJobInfos);
		} finally {
			try {
				if (connection!=null) {
					// 回收,并非关闭
					connection.close();
				}
			} catch (SQLException e) {
				log.error("connection close Exception:", e);
			}
		}
	}

	/**
	 * 列装换
	 * @param data
	 * @param relation
	 * @return
	 */
	private Map<String, ColumnValue> columnConvert(Map<String, ColumnValue> data, Map<String, ConvertInfo.ToColumn> relation) {
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
	 * 获取插入语句
	 * @param convertInfo
	 * @param data
	 * @return
	 */
	private String doGetInsertSql(ConvertInfo convertInfo, Map<String, ColumnValue> data) {
		String insertSql = convertInfo.getInsertSql();
		// 数据源中的各个表
		Map<String, ConvertInfo.ToColumn> relation = convertInfo.getTableColumns();
		Map<String, ColumnValue> mysqlData = columnConvert(data, relation);
		if (StringUtils.isBlank(insertSql)) {
			String tableName = convertInfo.getTableName();
			StringBuilder insertBuilder = new StringBuilder("INSERT IGNORE `");
			insertBuilder.append(tableName);
			insertBuilder.append("` (");
			StringBuilder placeholders = new StringBuilder(" VALUES(");
			mysqlData.keySet().forEach(column -> {
				insertBuilder.append("`").append(column).append("`").append(",");
				placeholders.append("%s").append(",");
			});
			insertBuilder.delete(insertBuilder.length() - 1, insertBuilder.length()).append(")");
			placeholders.delete(placeholders.length() - 1, placeholders.length()).append(")");
			insertSql = insertBuilder.append(placeholders).toString();
			convertInfo.setInsertSql(insertSql);
		}
		List<Object> args = new ArrayList<>();
		for (ColumnValue columnValue : mysqlData.values()) {
			args.add(getValue(columnValue));
		}
		return String.format(insertSql, args.toArray());
	}

	/**
	 * 获取值
	 * @param columnValue
	 * @return
	 */
	private String getValue(ColumnValue columnValue) {
		String value = columnValue.getValue();
		if (value==null) {
			return "NULL";
		} else {
			int sqlType = columnValue.getSqlType();
			if (Types.BIT == sqlType || Types.TINYINT == sqlType || Types.SMALLINT == sqlType || Types.INTEGER == sqlType || Types.BIGINT == sqlType ||Types.FLOAT == sqlType || Types.DOUBLE == sqlType || Types.NUMERIC == sqlType || Types.DECIMAL == sqlType) {
				return value;
			} else {
				return "'"+value.replace("'", "''")+"'";
			}
		}
	}

	/**
	 * 获取更新语句
	 * @param convertInfo
	 * @param updateData
	 * @param conditionData
	 * @return
	 */
	private String doGetUpdateSql(ConvertInfo convertInfo, Map<String, ColumnValue> updateData, Map<String, ColumnValue> conditionData) {
		// 数据源中的各个表
		Map<String, ConvertInfo.ToColumn> relation = convertInfo.getTableColumns();
		Map<String, ColumnValue> covertUpdateData = columnConvert(updateData, relation);
		Map<String, ColumnValue> covertConditionData = columnConvert(conditionData, relation);
		if (covertUpdateData.isEmpty() || covertConditionData.isEmpty()) {
			log.info("无需更新");
			return null;
		}
		String updateSql;
		String tableName = convertInfo.getTableName();
		StringBuilder updateBuilder = new StringBuilder("UPDATE `" + tableName + "` SET ");
		covertUpdateData.forEach((column, columnValue) -> updateBuilder.append("`").append(column).append("`")
				.append("=").append(getValue(columnValue)).append(","));
		updateBuilder.delete(updateBuilder.length() - 1, updateBuilder.length());
		String conditionSql = doGetConditionSql(convertInfo, covertConditionData.keySet());
		List<Object> args = new ArrayList<>();
		for (ColumnValue columnValue : conditionData.values()) {
			args.add(getValue(columnValue));
		}
		conditionSql = String.format(conditionSql, args.toArray());
		updateSql = updateBuilder.append(conditionSql).toString();
		return updateSql;
	}

	/**
	 * 获取条件语句
	 * @param convertInfo
	 * @param conditionColumns
	 * @return
	 */
	private String doGetConditionSql(ConvertInfo convertInfo, Set<String> conditionColumns) {
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

	/**
	 * 获取条件语句
	 * @param convertInfo
	 * @param conditionData
	 * @return
	 */
	private String doGetDeleteSql(ConvertInfo convertInfo, Map<String, ColumnValue> conditionData) {
		String deleteSql = convertInfo.getDeleteSql();
		Map<String, ConvertInfo.ToColumn> relation = convertInfo.getTableColumns();
		Map<String, ColumnValue> covertConditionData = columnConvert(conditionData, relation);
		if (covertConditionData.isEmpty()) {
			log.info("没有主键进行删除");
			return null;
		}
		if (StringUtils.isBlank(deleteSql)) {
			deleteSql = "DELETE FROM `" + convertInfo.getTableName() + "`" + doGetConditionSql(convertInfo, covertConditionData.keySet());
			convertInfo.setDeleteSql(deleteSql);
		}
		List<Object> args = new ArrayList<>();
		for (ColumnValue columnValue : conditionData.values()) {
			args.add(getValue(columnValue));
		}
		return String.format(deleteSql, args.toArray());
	}

	/**
	 * 保存失败的记录
	 * @param jobId
	 * @param canalJobInfos
	 */
	private void saveFailRecord(Integer jobId, List<CanalJobInfo> canalJobInfos) {
		if (CollectionUtils.isEmpty(canalJobInfos)) {
			return;
		}
		for (CanalJobInfo canalJobInfo : canalJobInfos) {
			IncrementUtil.saveWaiting(jobId, canalJobInfo.getEventType().name(), ProjectConstant.INCREMENT_SYNC_TYPE.CANAL, canalJobInfo.getUpdateColumns(), canalJobInfo.getConditionColumns(), canalJobInfo.getIdValue());
		}
	}

}

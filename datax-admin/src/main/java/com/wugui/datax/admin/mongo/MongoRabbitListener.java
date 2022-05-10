package com.wugui.datax.admin.mongo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.rabbitmq.client.Channel;
import com.wugui.datax.admin.constants.ProjectConstant;
import com.wugui.datax.admin.core.util.IncrementUtil;
import com.wugui.datax.admin.entity.ConvertInfo;
import com.wugui.datax.admin.util.MongoUtil;
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
import java.util.*;

/**
 * mongo异步消费
 * @author chen.ruihong
 * @date 2022/05/09
 */
@Slf4j
public class MongoRabbitListener implements ChannelAwareMessageListener {

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
		List<MongoJobInfo> mongoJobInfos = null;
		try {
			mongoJobInfos = MAPPER.readValue(body, new TypeReference<List<MongoJobInfo>>(){});
			ConvertInfo convertInfo = IncrementUtil.getConvertInfo(jobId);
			if (mongoJobInfos.isEmpty() || convertInfo == null) {
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
			List<String> sqlList = new ArrayList<>();
			String sql;
			for (MongoJobInfo mongoJobInfo : mongoJobInfos) {
				switch (mongoJobInfo.getEventType()) {
					case INSERT:
						sql = doGetInsertSql(convertInfo, mongoJobInfo.getUpdateColumns());
						if (StringUtils.isNotBlank(sql)) {
							statement.addBatch(sql);
							sqlList.add(sql);
						}
						break;
					case UPDATE:
						sql = doGetUpdateSql(convertInfo, mongoJobInfo.getUpdateColumns(), mongoJobInfo.getId());
						if (StringUtils.isNotBlank(sql)) {
							statement.addBatch(sql);
							sqlList.add(sql);
						}
						break;
					case DELETE:
						sql = doGetDeleteSql(convertInfo, mongoJobInfo.getId());
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
			if (connection!=null) {
				connection.rollback();
			}
			channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
			saveFailRecord(jobId, mongoJobInfos);
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
	 * 列转换: 从mongo 的列 转换为 mysql的列
	 *
	 * @param mongoData mongo的数据map
	 * @param relation 列转换关系
	 * @return 转换后的 mysql数据
	 */
	private static Map<String, Object> columnConvert(Map<String, Object> mongoData, Map<String, ConvertInfo.ToColumn> relation) {
		// 这里使用 TreeMap
		TreeMap<String, Object> mysqlData = new TreeMap<>();
		for (Map.Entry<String, ConvertInfo.ToColumn> entry : relation.entrySet()) {
			String fromColumn = entry.getKey();
			ConvertInfo.ToColumn toColumn = entry.getValue();
			if (mongoData.containsKey(fromColumn)) {
				mysqlData.put(toColumn.getName(), convertValue(toColumn, mongoData.get(fromColumn)));
			} else if (fromColumn.contains(".")) {
				String[] splits = fromColumn.split("\\.");
				Map<String, Object> map = null;
				String key = "";
				int index = 0;
				for (String split : splits) {
					index++;
					key += (StringUtils.isNotBlank(key)?".":"") + split;
					if (mongoData.get(key) instanceof Map) {
						map = (Map<String, Object>) mongoData.get(key);
						break;
					}
				}
				Object value = null;
				if(map!=null && index < splits.length) {
					for (int i = index; i < splits.length; i++) {
						String split = splits[i];
						value = map.get(split);
						if (value instanceof Map) {
							map = (Map<String, Object>) value;
						}
					}
					mysqlData.put(toColumn.getName(), convertValue(toColumn, value));
				}
			}
		}
		return mysqlData;
	}

	/**
	 * 数组类型
	 */
	private static final String ARRAY_TYPE = "array";
	/**
	 * 嵌入文档数组类型
	 */
	private static final String DOCUMENT_ARRAY_TYPE = "document.array";

	/**
	 * 值装换
	 * @param toColumn
	 * @param value
	 * @return
	 */
	private static Object convertValue(ConvertInfo.ToColumn toColumn, Object value) {
		if(value==null) {
			return null;
		}
		String type = toColumn.getFromType();
		try {
			if(value instanceof Collection) {
				if (ARRAY_TYPE.equals(type) || DOCUMENT_ARRAY_TYPE.equals(type)) {
					String splitter = toColumn.getSplitter();
					if (StringUtils.isBlank(splitter)) {
						splitter = ",";
					}
					return Joiner.on(splitter).join((Iterable<?>) value);
				} else {
					return JSON.toJSONString(value, SerializerFeature.WriteDateUseDateFormat);
				}
			} else if(!MongoUtil.isBaseType(value.getClass()))  {
				return JSON.toJSONString(value, SerializerFeature.WriteDateUseDateFormat);
			}
		} catch (Exception e) {
			return value;
		}
		return value;
	}

	/**
	 * 获取插入语句
	 * @param convertInfo
	 * @param mongoData
	 * @return
	 */
	private String doGetInsertSql(ConvertInfo convertInfo, Map<String, Object> mongoData) {
		// 数据源中的各个表
		Map<String, ConvertInfo.ToColumn> relation = convertInfo.getTableColumns();
		Map<String, Object> mysqlData = columnConvert(mongoData, relation);
		String tableName = convertInfo.getTableName();
		StringBuilder insertBuilder = new StringBuilder("INSERT IGNORE `");
		insertBuilder.append(tableName);
		insertBuilder.append("` (");
		StringBuilder placeholders = new StringBuilder(" VALUES(");
		mysqlData.forEach((column, value) -> {
				insertBuilder.append("`").append(column).append("`").append(",");
				placeholders.append(getValue(value)).append(",");
		});
		insertBuilder.delete(insertBuilder.length() - 1, insertBuilder.length()).append(")");
		placeholders.delete(placeholders.length() - 1, placeholders.length()).append(")");
		return insertBuilder.append(placeholders).toString();
	}

	/**
	 * 获取值
	 * @param value
	 * @return
	 */
	private String getValue(Object value) {
		if (value==null) {
			return "NULL";
		} else {
			if (value instanceof Number) {
				return value.toString();
			} else {
				return "'"+value+"'";
			}
		}
	}

	/**
	 * 获取更新语句
	 * @param convertInfo
	 * @param updateData
	 * @param idValue
	 * @return
	 */
	private String doGetUpdateSql(ConvertInfo convertInfo, Map<String, Object> updateData, String idValue) {

		// 数据源中的各个表
		Map<String, ConvertInfo.ToColumn> relation = convertInfo.getTableColumns();

		Map<String, Object> mysqlData = columnConvert(updateData, relation);
		// 每个表 执行sql
		String unionKey = MongoUtil.getUnionKey(relation);
		if (StringUtils.isBlank(unionKey)) {
			log.info("主键不明，无法更新");
			return null;
		}
		mysqlData.remove(unionKey);
		if (mysqlData.isEmpty()) {
			log.info("无需更新");
			return null;
		}
		String updateSql;
		String tableName = convertInfo.getTableName();
		StringBuilder updateBuilder = new StringBuilder("UPDATE `" + tableName + "` SET ");
		mysqlData.forEach((column, value) -> updateBuilder.append("`").append(column).append("`")
				.append("=").append(getValue(value)).append(","));
		updateBuilder.delete(updateBuilder.length() - 1, updateBuilder.length());
		String conditionSql = MysqlUtil.doGetConditionSql(convertInfo, Collections.singleton(unionKey));
		conditionSql = String.format(conditionSql, idValue);
		updateSql = updateBuilder.append(conditionSql).toString();
		return updateSql;
	}

	/**
	 * 获取条件语句
	 * @param convertInfo
	 * @param idValue
	 * @return
	 */
	private String doGetDeleteSql(ConvertInfo convertInfo, String idValue) {
		String deleteSql = convertInfo.getDeleteSql();
		// 数据源中的各个表
		Map<String, ConvertInfo.ToColumn> relation = convertInfo.getTableColumns();
		// 每个表 执行sql
		String unionKey = MongoUtil.getUnionKey(relation);
		if (StringUtils.isBlank(unionKey)) {
			log.info("主键不明，无法删除");
			return null;
		}
		if (StringUtils.isBlank(deleteSql)) {
			deleteSql = "DELETE FROM `" + convertInfo.getTableName() + "`" + MysqlUtil.doGetConditionSql(convertInfo, Collections.singleton(unionKey));
			convertInfo.setDeleteSql(deleteSql);
		}
		return String.format(deleteSql, idValue);
	}

	/**
	 * 保存失败的记录
	 * @param jobId
	 * @param mongoJobInfos
	 */
	private void saveFailRecord(Integer jobId, List<MongoJobInfo> mongoJobInfos) {
		if (CollectionUtils.isEmpty(mongoJobInfos)) {
			return;
		}
		for (MongoJobInfo mongoJobInfo : mongoJobInfos) {
			IncrementUtil.saveWaiting(jobId, mongoJobInfo.getEventType().name(), ProjectConstant.INCREMENT_SYNC_TYPE.MONGO_WATCH, mongoJobInfo.getUpdateColumns(), mongoJobInfo.getId(), mongoJobInfo.getIdValue());
		}
	}
}

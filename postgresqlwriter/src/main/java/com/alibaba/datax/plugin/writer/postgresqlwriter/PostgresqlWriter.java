package com.alibaba.datax.plugin.writer.postgresqlwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.JdbcConnectionFactory;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.fastjson2.JSONObject;

import jdk.internal.org.objectweb.asm.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresqlWriter extends Writer {
	private static final DataBaseType DATABASE_TYPE = DataBaseType.PostgreSQL;
	private static final Logger LOG = LoggerFactory.getLogger(PostgresqlWriter.class);

	public static class Job extends Writer.Job {
		private Configuration originalConfig = null;
		private CommonRdbmsWriter.Job commonRdbmsWriterMaster;

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();

			// warn：not like mysql, PostgreSQL only support insert mode, don't use
			String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
			autoFullIndex(this.originalConfig);

			Map<String, Object> indexColumns = this.originalConfig.getMap(Key.INDEX_COLUMNS);
			if("repalce".equals(writeMode.trim().toLowerCase())){
				if(Objects.isNull(indexColumns) || indexColumns.isEmpty()){
					writeMode = "insert";
					this.originalConfig.set(Key.WRITE_MODE, writeMode);
					LOG.warn("In job init(), writeMode to \"insert\" beacuse table unique no find");
				}
			}

			this.commonRdbmsWriterMaster = new CommonRdbmsWriter.Job(DATABASE_TYPE);
			this.commonRdbmsWriterMaster.init(this.originalConfig);

			if("replace".equals(writeMode.trim().toLowerCase())){
				indexColumns = this.originalConfig.getMap(Key.INDEX_COLUMNS);
				List<String> columns = this.originalConfig.getList(Key.COLUMN, String.class);
				columns = columns.stream()
					.map(column -> column.replace("\"", ""))
					.collect(Collectors.toList());
				List<String> columnsNoInCondition = getColumnsNoInCondition(indexColumns, columns);
				String writeDataSqlTemplate = PostgresqlWriterUtil.getPostgresqlWriteDataTemplater(columns, indexColumns, columnsNoInCondition);
				this.originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
				LOG.info(String.format("replace 模式下，生成的SQL模板为：%s", writeDataSqlTemplate));
			
			}
		}

		private List<String> getColumnsNoInCondition(Map<String, Object> indexColumns, List<String> columns) {
			List<String> columnsInCondition = new ArrayList<>();
			Iterator<Map.Entry<String, Object>> iterator = indexColumns.entrySet().iterator();
			while (iterator.hasNext()){
				Map.Entry<String, Object> entry = iterator.next();
				List<String> entryColumns = (List<String>) entry.getValue();
				for(String column : entryColumns){
					columnsInCondition.add(column);
				}
			}
			List<String> columnsNoInCondition = new ArrayList<>();
			for(String column : columns){
				if(!columnsInCondition.contains(column)){
					columnsNoInCondition.add(column);
				}
			}
			return columnsNoInCondition;
		}

		@Override
		public void prepare() {
			this.commonRdbmsWriterMaster.prepare(this.originalConfig);
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			return this.commonRdbmsWriterMaster.split(this.originalConfig, mandatoryNumber);
		}

		@Override
		public void post() {
			this.commonRdbmsWriterMaster.post(this.originalConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsWriterMaster.destroy(this.originalConfig);
		}

	}

	private static void autoFullIndex(Configuration originalConfig) {
		Map<String, Object> indexColumns = originalConfig.getMap(Key.INDEX_COLUMNS);
		if (Objects.isNull(indexColumns) || indexColumns.isEmpty()) {
			String  jdbcUrl = originalConfig.getString(String.format("%s[0].%s", Constant.CONN_MARK, Key.JDBC_URL));
			String username = originalConfig.getString(Key.USERNAME);
			String password = originalConfig.getString(Key.PASSWORD);
			String oneTabble = originalConfig.getString(String.format("%s[0].%s", Constant.CONN_MARK, Key.TABLE));
			String schema = null;
			String tabbleName = null;
			String[] tables = oneTabble.split("\\.");
			if (tables.length == 2){
				schema = tables[0];
				tabbleName = tables[1];
			} else if (tables.length == 1){
				tabbleName = tables[0];
			} else {
				throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR, String.format("表名：%s, 其中.符号数量过多无法辨别schema和table", oneTabble));
			}
			JdbcConnectionFactory JdbcConnectionFactory = new JdbcConnectionFactory(DATABASE_TYPE, jdbcUrl, username, password);
			Connection connection = null;
			try{
				connection = JdbcConnectionFactory.getConnecttion();
				Map<String, Object> indexName2List = PostgresqlWriterUtil.getIndexName2Columns(connection, schema, tabbleName);
				originalConfig.set(Key.INDEX_COLUMNS, new JSONObject(indexName2List));
				LOG.info(String.format("自动分析索引为：%s", indexName2List.toString()));
			} finally {
				DBUtil.closeDBResources(null, null, connection);
			}
			indexColumns = originalConfig.getMap(Key.INDEX_COLUMNS);
			List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
			columns = columns.stream()
				.map(column -> column.replace("\"", ""))
				.collect(Collectors.toList());
			PostgresqlWriterUtil.removeIndexNoInColumns(indexColumns, columns);
		}
	}

	public static class Task extends Writer.Task {
		private Configuration writerSliceConfig;
		private CommonRdbmsWriter.Task commonRdbmsWriterSlave;

		@Override
		public void init() {
			this.writerSliceConfig = super.getPluginJobConf();
			this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Task(DATABASE_TYPE){
				@Override
				protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex,
                                                                    int columnSqltype, String typeName, Column column) throws SQLException {
					switch (columnSqltype) {
						case Types.INTEGER:
							Long bigIntegerValue = column.asLong();
							if (emptyAsNull && Objects.isNull(bigIntegerValue)){
								preparedStatement.setNull(columnIndex + 1, columnSqltype);
							} else {
								preparedStatement.setInt(columnIndex + 1, bigIntegerValue.intValue());
							}
							break;
						case Types.SMALLINT:
						case Types.BIGINT:
						case Types.NUMERIC:
						case Types.DECIMAL:
						case Types.FLOAT:
						case Types.REAL:
						case Types.DOUBLE:
							Double doubleValue = column.asDouble();
							if (emptyAsNull && Objects.isNull(doubleValue)){
								preparedStatement.setNull(columnIndex + 1, columnSqltype);
							} else {
								preparedStatement.setDouble(columnIndex + 1, doubleValue);
							}
							break;
						case Types.BIT:
							Boolean boolValue = column.asBoolean();
							if (emptyAsNull && Objects.isNull(boolValue)) {
								preparedStatement.setNull(columnIndex + 1, columnSqltype);
							} else {
								preparedStatement.setBoolean(columnIndex + 1, boolValue.booleanValue());
							}
							break;
						case Type.ARRAY:
							String stringValue = column.asString();
							if (emptyAsNull && Objects.isNull(stringValue)) {
								preparedStatement.setNull(columnIndex + 1, columnSqltype);
							} else {
								preparedStatement.setString(columnIndex + 1, stringValue);
							}
							break;
						default:
							super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqltype, typeName, column);
            		}
            		return preparedStatement;
        		}
			};
			this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
		}

		@Override
		public void prepare() {
			this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
		}

		public void startWrite(RecordReceiver recordReceiver) {
			this.commonRdbmsWriterSlave.startWrite(recordReceiver, this.writerSliceConfig, super.getTaskPluginCollector());
		}

		@Override
		public void post() {
			this.commonRdbmsWriterSlave.post(this.writerSliceConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsWriterSlave.destroy(this.writerSliceConfig);
		}

	}

}

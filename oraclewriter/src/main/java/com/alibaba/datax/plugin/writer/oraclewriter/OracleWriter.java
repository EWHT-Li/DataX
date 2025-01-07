package com.alibaba.datax.plugin.writer.oraclewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.JdbcConnectionFactory;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.fastjson2.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;


public class OracleWriter extends Writer {
	private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;
	private static final Logger LOG = LoggerFactory
			.getLogger(CommonRdbmsWriter.Job.class);
	public static class Job extends Writer.Job {
		private Configuration originalConfig = null;
		private CommonRdbmsWriter.Job commonRdbmsWriterJob;

        public void preCheck() {
            this.init();
            this.commonRdbmsWriterJob.writerPreCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();

			// warn：not like mysql, oracle only support insert mode, don't use
			String writeMode = this.originalConfig.getString(Key.WRITE_MODE);

			autoFullIndex(this.originalConfig);

			//字段表
			Map<String, Object>  indexColumns = this.originalConfig.getMap(Key.INDEX_COLUMNS);
			if("replace".equals(writeMode.trim().toLowerCase())){
				//TODO 输出警告，因为模式可能发生变化
				if(Objects.isNull(indexColumns) || indexColumns.isEmpty()){
					writeMode = "insert";
					this.originalConfig.set(Key.WRITE_MODE, writeMode);
					LOG.warn("In job init(), writeMode to \"insert\" because table unique no find");
				}
			}

			this.commonRdbmsWriterJob = new CommonRdbmsWriter.Job(
					DATABASE_TYPE);
			this.commonRdbmsWriterJob.init(this.originalConfig);

			if("replace".equals(writeMode.trim().toLowerCase())){
				indexColumns = this.originalConfig.getMap(Key.INDEX_COLUMNS);
				List<String> columns = this.originalConfig.getList(Key.COLUMN, String.class);
				List<String> columnsNoInCondition = getColumnsNoInCondition(indexColumns, columns);
				String writeDataSqlTemplate = OracleWriterUtil.getOracleWriteDataSqlTemplater(columns, indexColumns, columnsNoInCondition);
				this.originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
				LOG.info("Write data sql change to [\n{}\n]", writeDataSqlTemplate);
			}
		}

		private List<String> getColumnsNoInCondition(Map<String, Object> indexColumns, List<String> columns) {
			List<String> columnsInCondition = new ArrayList<>();
			Iterator<Map.Entry<String, Object>> iterator = indexColumns.entrySet().iterator();
			while (iterator.hasNext()){
				Map.Entry<String, Object> entry = iterator.next();
				ArrayList<String> entryColumns = (ArrayList<String>) entry.getValue();
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
            //oracle实跑先不做权限检查
            //this.commonRdbmsWriterJob.privilegeValid(this.originalConfig, DATABASE_TYPE);
			this.commonRdbmsWriterJob.prepare(this.originalConfig);
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			return this.commonRdbmsWriterJob.split(this.originalConfig,
					mandatoryNumber);
		}

		@Override
		public void post() {
			this.commonRdbmsWriterJob.post(this.originalConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsWriterJob.destroy(this.originalConfig);
		}

	}

	//按Map<String, List<String>>填充索引信息到 indexColumns 下
	private static void autoFullIndex(Configuration originalConfig) {
		Map<String, Object> indexColumns = originalConfig.getMap(Key.INDEX_COLUMNS);
		if(Objects.isNull(indexColumns) || indexColumns.isEmpty()){
			String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
					Constant.CONN_MARK, Key.JDBC_URL));
			String username = originalConfig.getString(Key.USERNAME);
			String password = originalConfig.getString(Key.PASSWORD);
			String oneTable = originalConfig.getString(String.format(
					"%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE));
			oneTable = oneTable.replace("\"","");
			String schema = null;
			String tableName = null;
			String[] tables = oneTable.split("\\.");
			if(tables.length == 2){
				schema = tables[0];
				tableName = tables[1];
			}else if(tables.length == 1) {
				tableName = tables[0];
			}else {
				throw DataXException
							.asDataXException(
									DBUtilErrorCode.CONF_ERROR,
									String.format(
											"表名为%s, 其中.符号可能过多无法辨别schema和table",
											oneTable));
			}
			JdbcConnectionFactory jdbcConnectionFactory = new JdbcConnectionFactory(DATABASE_TYPE, jdbcUrl, username, password);
			Connection connection = null;
			try {
				connection = jdbcConnectionFactory.getConnecttion();
				HashMap<String, Object> indexName2List = OracleWriterUtil.getIndexName2Columns(connection, schema, tableName);
				originalConfig.set(Key.INDEX_COLUMNS, new JSONObject(indexName2List));
				LOG.info(String.format("自动分析索引为 %s", indexName2List.toString()));
			} finally {
				DBUtil.closeDBResources(null,null, connection);
			}
			indexColumns = originalConfig.getMap(Key.INDEX_COLUMNS);
			List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
			OracleWriterUtil.removeIndexNoInColumns(indexColumns, columns);
		}
	}

	public static class Task extends Writer.Task {
		private Configuration writerSliceConfig;
		private CommonRdbmsWriter.Task commonRdbmsWriterTask;

		@Override
		public void init() {
			this.writerSliceConfig = super.getPluginJobConf();
			this.commonRdbmsWriterTask = new CommonRdbmsWriter.Task(DATABASE_TYPE);
			this.commonRdbmsWriterTask.init(this.writerSliceConfig);
		}

		@Override
		public void prepare() {
			this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
		}

		public void startWrite(RecordReceiver recordReceiver) {
			this.commonRdbmsWriterTask.startWrite(recordReceiver,
					this.writerSliceConfig, super.getTaskPluginCollector());
		}

		@Override
		public void post() {
			this.commonRdbmsWriterTask.post(this.writerSliceConfig);
		}

		@Override
		public void destroy() {
			this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
		}

	}

}
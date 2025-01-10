package com.alibaba.datax.plugin.writer.postgresqlwriter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;

import java.util.Iterator;

public class PostgresqlWriterUtil {

    static String getPostgresqlWriteDataTemplater(List<String> columns, Map<String, Object> indexColumns, List<String> columnsNoInCondition) {
        StringBuilder insertSQLWithInsertTemplate = new StringBuilder();
        StringBuilder insertSQLWithValuesTemplate = new StringBuilder();
        boolean first = true;
        for (String column : columns) {
            if (first) {
                first = false;
            } else {
                insertSQLWithInsertTemplate.append(",");
                insertSQLWithValuesTemplate.append(",");
            }
            insertSQLWithInsertTemplate.append("\"").append(column).append("\"");
            insertSQLWithValuesTemplate.append("?");
        }
        StringBuilder onSQLTemplate = new StringBuilder();
        for(String index: indexColumns.keySet()){
            List<String> indexColumnsList = (List<String>) indexColumns.get(index);
            for(String indexColumn: indexColumnsList){
                onSQLTemplate.append("\"").append(indexColumn).append("\"").append(" = ? ");
            }
        }
        StringBuilder updateSQLTemplate = new StringBuilder();
        first = true;
        for (String column : columnsNoInCondition) {
            if (first) {
                first = false;
            } else {
                updateSQLTemplate.append(",");
            }
            updateSQLTemplate.append("\"").append(column).append("\"")
            .append("= EXCLUDED.")
            .append("\"").append(column).append("\" ");
        }

        String writeDataSqlTemplate = new StringBuilder()
            .append("INSERT INTO %s ( ")
            .append(insertSQLWithInsertTemplate).append(" ) ")
            .append("VALUES ( ")
            .append(insertSQLWithValuesTemplate).append(" ) ")
            .append("ON CONFILICT ( ")
            .append(onSQLTemplate).append(" ) ")
            .append("DO UPDATE SET ")
            .append(updateSQLTemplate)
            .toString();
        return writeDataSqlTemplate;
    }

    static void removeIndexNoInColumns(Map<String, Object> indexColumns, List<String> columns){
        Iterator<Map.Entry<String, Object>> iterater = indexColumns.entrySet().iterator();
        HashSet<String> indexColumnsSet = new HashSet<>();
        while(iterater.hasNext()){
            Map.Entry<String, Object> entry = iterater.next();
            List<String> indexColumnsList = (List<String>) entry.getValue();
            for(String indexColumn: indexColumnsList){
                if(!columns.contains(indexColumn)){
                    iterater.remove();
                    break;
                }
            }
        }

    }

    static HashMap<String, Object> getIndexName2Columns(Connection connection, String schema, String tableName) {
        ResultSet pkRs = null;
        ResultSet indexRs = null;
        HashMap<String, Object> indexName2Columns = new HashMap<>();
        try {
            pkRs = connection.getMetaData().getPrimaryKeys(null, schema, tableName);
            while (pkRs.next()) {
                String pkName = pkRs.getString("PK_NAME");
                String columnName = pkRs.getString("COLUMN_NAME");
                HashSet<String> columnNames = (HashSet<String>) indexName2Columns.getOrDefault(pkName, new HashSet<String>());
                columnNames.add(columnName);
                indexName2Columns.put(pkName, columnNames);
            }
            indexRs = connection.getMetaData().getIndexInfo(null, schema, tableName, true, true);
            while (indexRs.next()) {
                String indexName = indexRs.getString("INDEX_NAME");
                String columnName = indexRs.getString("COLUMN_NAME");
                if(Objects.isNull(columnName) || Objects.isNull(indexName)){
                    continue;
                }
                HashSet<String> columnNames = (HashSet<String>) indexName2Columns.getOrDefault(indexName, new HashSet<String>());
                columnNames.add(columnName);
                indexName2Columns.put(indexName, columnNames);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            DBUtil.closeDBResources(pkRs, null, null);
            DBUtil.closeDBResources(indexRs, null, null);
        }
        return indexName2Columns;
    }
    
}

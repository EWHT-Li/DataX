package com.alibaba;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class OracleWriteUtil {
    
    static String getOracleWriteDataSqlTemplater(List<String> columns, Map<String, Object> indexColumns, List<String> columnsNoInCondition){
        StringBuilder selectSQLTemplate = new StringBuilder();
        StringBuilder insertSQLWithInsertTemplate = new StringBuilder();
        StringBuilder insertSQLWithValueTemplate = new StringBuilder();
        boolean first = true;
        selectSQLTemplate.append("SELECT ");
        for(String column : columns){
            if(!first){
                selectSQLTemplate.append(", ");
                insertSQLWithInsertTemplate.append(", ");
                insertSQLWithValueTemplate.append(", ");
            } else{
                first = false;
            }
            selectSQLTemplate.append("? AS\"").append(column).append("\" ");

            insertSQLWithInsertTemplate.append("\"").append(column).append("\" ");
            insertSQLWithValueTemplate.append("b.").append("\"").append(column).append("\" ");  
        }
        selectSQLTemplate.append("FROM dual ");

        StringBuilder onSQLTemplate = new StringBuilder();
        boolean conditionFirst = true;
        for(String index: indexColumns.keySet()){
            ArrayList<String> columns1 = (ArrayList<String>) indexColumns.get(index);
            if(!conditionFirst){
                onSQLTemplate.append("OR ");
            } else{
                conditionFirst = false;
            }
            onSQLTemplate.append("( ");
            boolean columnFirst = true;
            for(String column : columns1){
                if(!columnFirst){
                    onSQLTemplate.append("AND ");
                } else{
                    columnFirst = false;
                }
                onSQLTemplate.append("( ");
                onSQLTemplate.append("b.\"").append(column).append("\" ");
                onSQLTemplate.append("= ");
                onSQLTemplate.append("a.\"").append(column).append("\" ");
                onSQLTemplate.append(") ");
            }
            onSQLTemplate.append(") ");
        }

        StringBuilder updateSQLTemplate = new StringBuilder();
        updateSQLTemplate.append("UPDATE SET ");
        first = true;
        for (String column : columnsNoInCondition){
            if(first){
                first = false;
            } else{
                updateSQLTemplate.append(", ");
            }
            updateSQLTemplate.append("\"").append(column).append("\" = ");
            updateSQLTemplate.append("b.\"").append(column).append("\" ");
        }

        String writeDateSqlTemplate = new StringBuilder()
            .append("MERGE INTO %s a USING ( ").append(selectSQLTemplate).append(" ) b ON ( ").append(onSQLTemplate).append(" ) ")
            .append("WHEN MATCHED THEN ").append(updateSQLTemplate)
            .append(" WHEN NOT MATCHED THEN ")
            .append("INSERT ( ").append(insertSQLWithInsertTemplate).append(" ) ")
            .append("VALUES ").append(insertSQLWithValueTemplate).append(" ) ")
            .toString();
        return writeDateSqlTemplate;
    }

    static void removeIndexNoInColumns(Map<String, Object> indexColumns, List<String> columns){
        Iterator<Map.Entry<String, Object>> iterator = indexColumns.entrySet().iterator();
        HashSet<String> columnSet = new HashSet<>(columns);
        while (iterator.hasNext()){
            Map.Entry<String, Object> entry = iterator.next();
            ArrayList<String> indexColumn = (ArrayList<String>) entry.getValue();
            for(String column : indexColumn){
                if(!columnSet.contains(column)){
                    iterator.remove();
                    break;
                }
            }
        }
    }

    static HashMap<String, Object> getIndexName2Columns(Connection connection, String schema, String tableName) {
        ResultSet pkRs = null;
        ResultSet indexRs = null;
        HashMap<String, Object> indexName2List = new HashMap<>();
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            pkRs = metaData.getPrimaryKeys(null, schema, tableName);
            while (pkRs.next()) {
                String primaryKeyName = pkRs.getString("PK_NAME");
                HashSet<String> columnNames = (HashSet<String>) indexName2List.getOrDefault(primaryKeyName, new HashSet<String>());
                String columnName = pkRs.getString("COLUMN_NAME");
                columnNames.add(columnName);
                indexName2List.put(primaryKeyName, columnNames);
            }
            indexRs = metaData.getIndexInfo(null, schema, tableName, true, true);
            while (indexRs.next()) {
                String indexName = indexRs.getString("INDEX_NAME");
                HashSet<String> columnNames = (HashSet<String>) indexName2List.getOrDefault(indexName, new HashSet<String>());
                String columnName = indexRs.getString("COLUMN_NAME");
                if (Objects.isNull(indexName) || Objects.isNull(columnNames)){
                    continue;
                }
                columnNames.add(columnName);
                indexName2List.put(indexName, columnNames);
            }
            indexName2List.forEach((key, value) -> {
                indexName2List.put(key, new ArrayList<>((HashSet)(value));
            });

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DBUtil.closeDBResources(pkRs, null, null);
            DBUtil.closeDBResources(indexRs, null, null);
        }
        return indexName2List;
    }
}

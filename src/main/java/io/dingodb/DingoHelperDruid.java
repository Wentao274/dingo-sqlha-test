/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb;

import org.apache.commons.io.IOUtils;

import javax.annotation.Nonnull;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DingoHelperDruid {
    public DingoHelperDruid() {}
    
    public void execFile(@Nonnull InputStream stream, String replaceTableName) throws IOException, SQLException {
        Connection connection = null;
        Statement statement = null;
        String sql = IOUtils.toString(stream, StandardCharsets.UTF_8);
        String exeSql = sql.replace("$table", replaceTableName);
        try{
            connection = DruidUtilsDingo.getDruidDingoConnection();
            statement = connection.createStatement();
            execSql(statement, exeSql);
        } finally {
            DruidUtilsDingo.closeResource(connection, null, statement);
        }
    }

    public static void execSql(Statement statement, @Nonnull String sql) throws SQLException {
        for (String s : sql.split(";")) {
            if (!s.trim().isEmpty()) {
                statement.execute(s);
            }
        }
    }

    public void execSql(String sql) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            statement = connection.createStatement();
            if (!sql.trim().isEmpty()) {
                statement.execute(sql);
            }
        } finally {
            DruidUtilsDingo.closeResource(connection, null, statement);
        }
    }

    public void execBatchSqlWithState(String sql) throws SQLException, InterruptedException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            statement = connection.createStatement();
            for (String s : sql.split(";")) {
                if (!s.trim().isEmpty()) {
                    statement.execute(s);
                }
            }
        } finally {
            DruidUtilsDingo.closeResource(connection, null, statement);
        }
    }

    public static void execBatchSql(Statement statement, String sql) throws SQLException {
        for (String s : sql.split(";")) {
            if (!s.trim().isEmpty()) {
                statement.execute(s);
            }
        }
    }
    

    //dml操作只返回影响行数
    public int doDMLReturnRows(String sql) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            statement = connection.createStatement();
            int rowCount = statement.executeUpdate(sql);
            return rowCount;
        } finally {
            DruidUtilsDingo.closeResource(connection, null, statement);
        }
    }

    //更新或删除表数据并查询变更后数据，输出包含表头
    public List<List<String>> doDMLAndQueryWithHead(String dmlSql, String querySql) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            statement = connection.createStatement();
            List<List<String>> rowList = new ArrayList<>();
            List<String> effectRows = new ArrayList<>();
            int effectCnt = statement.executeUpdate(dmlSql);
            effectRows.add(String.valueOf(effectCnt));
            rowList.add(effectRows);

            //更新后查询表数据
            resultSet = statement.executeQuery(querySql);
            List<List<String>> resultList = getResultListWithLabel(resultSet, rowList);
            return resultList;
        } finally {
            DruidUtilsDingo.closeResource(connection, resultSet, statement);
        }
    }
    
    //查询返回结果行数
    public int queryWithRowsNumReturn(String sql) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            int rowCount = 0;
            while (resultSet.next()) {
                rowCount ++;
            }
            return rowCount;
        } finally {
            DruidUtilsDingo.closeResource(connection, resultSet, statement);
        }
    }
    

    //通过statement查询表数据, 返回List<List<String>>,包含输出表头
    public List<List<String>> statementQueryWithHead(String sql) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            List<List<String>> resultList = getResultListWithLabel(resultSet);
            return resultList;
        } finally {
            DruidUtilsDingo.closeResource(connection, resultSet, statement);
        }
    }
    

    public void doDropTable(String tableName) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            statement = connection.createStatement();
            String sql = "drop table if exists " + tableName;
            statement.execute(sql);
        } finally {
            DruidUtilsDingo.closeResource(connection, null, statement);
        }
    }

    public void doDropSchema(String schemaName) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            statement = connection.createStatement();
            String sql = "drop schema if exists " + schemaName;
            statement.execute(sql);
        } finally {
            DruidUtilsDingo.closeResource(connection, null, statement);
        }
    }

    public void doDropDatabase(String databaseName) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            statement = connection.createStatement();
            String sql = "drop database if exists " + databaseName;
            statement.execute(sql);
        } finally {
            DruidUtilsDingo.closeResource(connection, null, statement);
        }
    }
    public void doDropUser(String userName) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            statement = connection.createStatement();
            String sql = "drop user if exists " + userName;
            statement.execute(sql);
        } finally {
            DruidUtilsDingo.closeResource(connection, null, statement);
        }
    }

    //通过指定列索引查询表数据, 返回List<List<String>>,包含输出表头
    public List<List<String>> statementQueryWithSpecifiedColIndex(String sql, List<Integer> colIndexList) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            List<List<String>> resultList = getResultListWithColumnIndex(resultSet, colIndexList);
            return resultList;
        } finally {
            DruidUtilsDingo.closeResource(connection, resultSet, statement);
        }
    }

    //插入Blob类型字段的数据
    public int preparedStatementInsertBlobData(String insert_sql, String[] insert_value_type, List insert_values) throws SQLException {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            ps = connection.prepareStatement(insert_sql);
            for (int i = 0; i < insert_values.size(); i++) {
                switch (insert_value_type[i].trim()) {
                    case "Varchar": {
                        ps.setString(i + 1, (String) insert_values.get(i));
                        break;
                    }
                    case "Integer": {
                        ps.setInt(i + 1, Integer.parseInt((String) insert_values.get(i)));
                        break;
                    }
                    case "Bigint": {
                        ps.setLong(i + 1, Long.parseLong((String) insert_values.get(i)));
                        break;
                    }
                    case "Blob": {
                        ps.setBytes(i + 1, (byte[]) insert_values.get(i));
                        break;
                    }
                }
            }
            int effect_rows = ps.executeUpdate();
            return effect_rows;
        } finally {
            DruidUtilsDingo.closeResourcePS(connection, null, ps);
        }
    }

    //读取Blob类型字段的数据
    public FileOutputStream preparedStatementGetBlobData(String query_sql, String[] query_value_type, int blobIndex, String fileOutPath, Object ... query_values) throws SQLException, IOException {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        FileOutputStream fileOutputStream = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            ps = connection.prepareStatement(query_sql);
            for (int i = 0; i < query_values.length; i++) {
                switch (query_value_type[i].trim()) {
                    case "Varchar": {
                        ps.setString(i + 1, (String) query_values[i]);
                        break;
                    }
                    case "Integer": {
                        ps.setInt(i + 1, Integer.parseInt((String) query_values[i]));
                        break;
                    }
                    case "Bigint": {
                        ps.setLong(i + 1, Long.parseLong((String) query_values[i]));
                        break;
                    }
                    case "Blob": {
                        ps.setBytes(i + 1, (byte[]) query_values[i]);
                        break;
                    }
                }
            }
            resultSet = ps.executeQuery();
            while (resultSet.next()) {
                byte[] bytes = resultSet.getBytes(blobIndex);
                fileOutputStream = new FileOutputStream(fileOutPath);
                fileOutputStream.write(bytes);
            }
            if (fileOutputStream != null) {
                fileOutputStream.close();
            }
            return fileOutputStream;
        } finally {
            DruidUtilsDingo.closeResourcePS(connection, resultSet, ps);
        }
    }

    //获取查询结果集，包含表头
    public List<List<String>> getResultListWithLabel(ResultSet resultSet, List<List<String>> resultList) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<String> lableList = new ArrayList<>();
//        List<String> typeList = new ArrayList<>();
        int columnCount = resultSetMetaData.getColumnCount();
        for (int l = 1; l <= columnCount; l++) {
            lableList.add(resultSetMetaData.getColumnLabel(l));
//            typeList.add(resultSetMetaData.getColumnTypeName(l));
        }
        resultList.add(lableList);

        while (resultSet.next()) {
            List rowList = new ArrayList();
            for (int i = 1; i <= columnCount; i++) {
                String columnLabel = resultSetMetaData.getColumnLabel(i);
                String columnTypeName = resultSetMetaData.getColumnTypeName(i);
                if (resultSet.getObject(columnLabel) == null) {
                    rowList.add(String.valueOf(resultSet.getObject(columnLabel)));
                } else if (resultSet.getObject(columnLabel).getClass().toString().equalsIgnoreCase("class [B")) {
                    rowList.add(Arrays.toString((byte[]) resultSet.getObject(columnLabel)));
                } else if (columnTypeName.equalsIgnoreCase("ARRAY")) {
                    rowList.add(resultSet.getArray(columnLabel).toString());
                } else if (columnTypeName.equalsIgnoreCase("DATE")) {
                    rowList.add(resultSet.getDate(columnLabel).toString().substring(0,10));
                } else if (columnTypeName.equalsIgnoreCase("TIME")) {
                    rowList.add(resultSet.getTime(columnLabel).toString().substring(0,8));
                } else if (columnTypeName.equalsIgnoreCase("TIMESTAMP")) {
                    rowList.add(resultSet.getTimestamp(columnLabel).toString().substring(0,19));
                } else {
                    rowList.add(resultSet.getObject(columnLabel).toString());
                }
            }
            resultList.add(rowList);
        }
        return resultList;
    }
    
    
    //获取查询结果集，包含表头
    public List<List<String>> getResultListWithLabel(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<List<String>> resultList = new ArrayList<>();
        List<String> lableList = new ArrayList<>();
//        List<String> typeList = new ArrayList<>();
        int columnCount = resultSetMetaData.getColumnCount();
        for (int l = 1; l <= columnCount; l++) {
            lableList.add(resultSetMetaData.getColumnLabel(l));
//            typeList.add(resultSetMetaData.getColumnTypeName(l));
        }
        resultList.add(lableList);
        
        while (resultSet.next()) {
            List rowList = new ArrayList();
            for (int i = 1; i <= columnCount; i++) {
                String columnLabel = resultSetMetaData.getColumnLabel(i);
                String columnTypeName = resultSetMetaData.getColumnTypeName(i);
                if (resultSet.getObject(columnLabel) == null) {
                    rowList.add(String.valueOf(resultSet.getObject(columnLabel)));
                } else if (resultSet.getObject(columnLabel).getClass().toString().equalsIgnoreCase("class [B")) {
                    rowList.add(Arrays.toString((byte[]) resultSet.getObject(columnLabel)));
                } else if (columnTypeName.equalsIgnoreCase("ARRAY")) {
                    rowList.add(resultSet.getArray(columnLabel).toString());
                } else if (columnTypeName.equalsIgnoreCase("DATE")) {
                    rowList.add(resultSet.getDate(columnLabel).toString().substring(0,10));
                } else if (columnTypeName.equalsIgnoreCase("TIME")) {
                    rowList.add(resultSet.getTime(columnLabel).toString().substring(0,8));
                } else if (columnTypeName.equalsIgnoreCase("TIMESTAMP")) {
                    rowList.add(resultSet.getTimestamp(columnLabel).toString().substring(0,19));
                } else {
                    rowList.add(resultSet.getObject(columnLabel).toString());
                }
            }
            resultList.add(rowList);
        }
        return resultList;
    }

    //获取查询结果集，不包含表头
    public List<List<String>> getResultListWithoutLabel(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<List<String>> resultList = new ArrayList<>();
        List<String> lableList = new ArrayList<>();
//        List<String> typeList = new ArrayList<>();
        int columnCount = resultSetMetaData.getColumnCount();
        for (int l = 1; l <= columnCount; l++) {
            lableList.add(resultSetMetaData.getColumnLabel(l));
//            typeList.add(resultSetMetaData.getColumnTypeName(l));
        }
        
        while (resultSet.next()) {
            List rowList = new ArrayList();
            for (int i = 1; i <= columnCount; i++) {
                String columnLabel = resultSetMetaData.getColumnLabel(i);
                String columnTypeName = resultSetMetaData.getColumnTypeName(i);
                if (resultSet.getObject(columnLabel) == null) {
                    rowList.add(String.valueOf(resultSet.getObject(columnLabel)));
                } else if (resultSet.getObject(columnLabel).getClass().toString().equalsIgnoreCase("class [B")) {
                    rowList.add(Arrays.toString((byte[]) resultSet.getObject(columnLabel)));
                } else if (columnTypeName.equalsIgnoreCase("ARRAY")) {
                    rowList.add(resultSet.getArray(columnLabel).toString());
                } else if (columnTypeName.equalsIgnoreCase("DATE")) {
                    rowList.add(resultSet.getDate(columnLabel).toString().substring(0,10));
                } else if (columnTypeName.equalsIgnoreCase("TIME")) {
                    rowList.add(resultSet.getTime(columnLabel).toString().substring(0,8));
                } else if (columnTypeName.equalsIgnoreCase("TIMESTAMP")) {
                    rowList.add(resultSet.getTimestamp(columnLabel).toString().substring(0,19));
                } else {
                    rowList.add(resultSet.getObject(columnLabel).toString());
                }
            }
            resultList.add(rowList);
        }
        return resultList;
    }

    //通过指定列索引获取查询结果集，包含表头
    public List<List<String>> getResultListWithColumnIndex(ResultSet resultSet, List<Integer> colIndexList) throws SQLException {
        List<List<String>> resultList = new ArrayList<>();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<String> lableList = new ArrayList<>();
//        int columnCount = resultSetMetaData.getColumnCount();
        for (int l = 0; l < colIndexList.size(); l++) {
            lableList.add(resultSetMetaData.getColumnLabel(colIndexList.get(l)));
        }
        resultList.add(lableList);

        while (resultSet.next()) {
            List rowList = new ArrayList();
            for (int i = 0; i < colIndexList.size(); i++) {
                String columnTypeName = resultSetMetaData.getColumnTypeName(colIndexList.get(i));
                if (resultSet.getObject(colIndexList.get(i)) == null) {
                    rowList.add(String.valueOf(resultSet.getObject(colIndexList.get(i))));
                } else if (resultSet.getObject(colIndexList.get(i)).getClass().toString().equalsIgnoreCase("class [B")) {
                    rowList.add(Arrays.toString((byte[]) resultSet.getObject(colIndexList.get(i))));
                } else if (columnTypeName.equalsIgnoreCase("ARRAY")) {
                    rowList.add(resultSet.getArray(colIndexList.get(i)).toString());
                } else if (columnTypeName.equalsIgnoreCase("DATE")) {
                    rowList.add(resultSet.getDate(colIndexList.get(i)).toString().substring(0,10));
                } else if (columnTypeName.equalsIgnoreCase("TIME")) {
                    rowList.add(resultSet.getTime(colIndexList.get(i)).toString().substring(0,8));
                } else if (columnTypeName.equalsIgnoreCase("TIMESTAMP")) {
                    rowList.add(resultSet.getTimestamp(colIndexList.get(i)).toString().substring(0,19));
                } else {
                    int startNum = resultSet.getObject(colIndexList.get(i)).toString().indexOf("[");
                    if (startNum >= 0) {
                        rowList.add(resultSet.getObject(colIndexList.get(i)).toString().substring(startNum));
                    } else {
                        rowList.add(resultSet.getObject(colIndexList.get(i)).toString());
                    }
                }
            }
            resultList.add(rowList);
        }
        return resultList;
    }
    
}

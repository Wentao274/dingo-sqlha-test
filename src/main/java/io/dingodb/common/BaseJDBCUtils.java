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

package io.dingodb.common;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class BaseJDBCUtils {
    public static List<String> baseGetSchemaList(Connection connection) {
        ResultSet resultSetSchema = null;
        List<String> schemaList = new ArrayList<>();
        try {
            DatabaseMetaData dmd = connection.getMetaData();
            resultSetSchema = dmd.getSchemas();
            while (resultSetSchema.next()) {
                schemaList.add(resultSetSchema.getString(1).toUpperCase());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeResource(connection, resultSetSchema,null);
        }
        return schemaList;
    }

    public static List<String> baseGetTableList(Connection connection, String schemaStr) {
        DatabaseMetaData dmd = null;
        ResultSet resultSetSchema = null;
        ResultSet resultSetTable = null;
        List<String> schemaList = new ArrayList<>();
        List<String> tableList = new ArrayList<>();
        try {
            dmd = connection.getMetaData();
            resultSetSchema = dmd.getSchemas();
            while (resultSetSchema.next()) {
                schemaList.add(resultSetSchema.getString(1));
            }
            String[] types={"TABLE"};
            resultSetTable = dmd.getTables(null, schemaStr, "%", types);
            while (resultSetTable.next()) {
                tableList.add(resultSetTable.getString("TABLE_NAME").toUpperCase());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeResourceMultiRst(connection, resultSetTable, resultSetSchema);
        }
        return tableList;
    }

    public static List<String> baseGetTableListWithSchema(Connection connection, String schemaName) {
        DatabaseMetaData dmd = null;
        ResultSet resultSetSchema = null;
        ResultSet resultSetTable = null;
        List<String> schemaList = new ArrayList<>();
        List<String> tableList = new ArrayList<>();
        try {
            dmd = connection.getMetaData();
            resultSetSchema = dmd.getSchemas();
            while (resultSetSchema.next()) {
                schemaList.add(resultSetSchema.getString(1));
            }
            String[] types={"TABLE"};
            resultSetTable = dmd.getTables(null, schemaName, "%", types);
            while (resultSetTable.next()) {
                tableList.add(resultSetTable.getString("TABLE_NAME").toUpperCase());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeResourceMultiRst(connection, resultSetTable, resultSetSchema);
        }
        return tableList;
    }

    public static List<String> baseGetAllTableList(Connection connection) {
        DatabaseMetaData dmd = null;
        ResultSet resultSetSchema = null;
        ResultSet resultSetTable = null;
        List<String> schemaList = new ArrayList<>();
        List<String> tableList = new ArrayList<>();
        try {
            dmd = connection.getMetaData();
            resultSetSchema = dmd.getSchemas();
            while (resultSetSchema.next()) {
                schemaList.add(resultSetSchema.getString(1));
            }
            String[] types={"TABLE"};
            resultSetTable = dmd.getTables(null, null, "%", types);
            while (resultSetTable.next()) {
                tableList.add(resultSetTable.getString("TABLE_NAME").toUpperCase());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeResourceMultiRst(connection, resultSetTable, resultSetSchema);
        }
        return tableList;
    }
    public static void closeResource(Connection connection, ResultSet rs, Statement stm) {
        try{
            if(rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            if(stm != null) {
                stm.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            if(connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void closeResourcePS(Connection connection, ResultSet rs, PreparedStatement ps) {
        try{
            if(rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            if(ps != null) {
                ps.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            if(connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void closeResourceMultiRst(Connection connection, ResultSet rst1, ResultSet rst2) {
        try{
            if(rst1 != null) {
                rst1.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            if(rst2 != null) {
                rst2.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            if(connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

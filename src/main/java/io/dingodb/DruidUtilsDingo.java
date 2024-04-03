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

import com.alibaba.druid.pool.DruidDataSourceFactory;
import io.dingodb.common.BaseJDBCUtils;
import io.dingodb.common.CommonArgs;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class DruidUtilsDingo extends BaseJDBCUtils {
    static final String USER = CommonArgs.getDefaultConnectUser();
    static final String PASS = CommonArgs.getDefaultConnectPass();
    static final String SCHEMANAME = "DINGO";
    private static DataSource source;
    static {
        Properties properties = null;
        try {
            properties = new Properties();
            InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream("druid_dingo.properties");
            properties.load(inputStream);
            String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
//            String defaultConnectIP = "172.20.61.101";
            String port = CommonArgs.getDefaultExecutorPort();
//            String jdbcUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":" + port + "/" + SCHEMANAME + "?usingRelOp=false&pushDown=false&timeout=600";
            String jdbcUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":" + port + "/" + SCHEMANAME + "?timeout=1500";
            properties.setProperty("url", jdbcUrl);
            properties.setProperty("username", USER);
            properties.setProperty("password", PASS);
            String poolSize = String.valueOf(Runtime.getRuntime().availableProcessors() * 1);
            System.out.println("Dingo connection pool size: " + poolSize);
            properties.setProperty("initialSize", poolSize);
            properties.setProperty("maxActive", poolSize);
            source = DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static Connection getDruidDingoConnection() throws SQLException {
        Connection connection = source.getConnection();
        return connection;
    }

    private static DataSource sourceNoSchema;

    public static Connection getDruidDingoConnectionWithoutSchema() throws SQLException {
        Properties properties = null;
        try {
            properties = new Properties();
            InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream("druid_dingo.properties");
            properties.load(inputStream);
            String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
            String port = CommonArgs.getDefaultExecutorPort();
            String jdbcUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":" + port;
            properties.setProperty("url", jdbcUrl);
            properties.setProperty("username", USER);
            properties.setProperty("password", PASS);
            properties.setProperty("initialSize", "2");
            properties.setProperty("maxActive", "2");
            sourceNoSchema = DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Connection connection = sourceNoSchema.getConnection();
        return connection;
    }

    private static DataSource sourceWithSchema;
    public static Connection getDruidDingoConnectionWithSchema(String schemaName) throws SQLException {
        Properties properties = null;
        try {
            properties = new Properties();
            InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream("druid_dingo.properties");
            properties.load(inputStream);
            String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
            String port = CommonArgs.getDefaultExecutorPort();
            String jdbcUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":" + port + "/" + schemaName;
            properties.setProperty("url", jdbcUrl);
            properties.setProperty("username", USER);
            properties.setProperty("password", PASS);
            properties.setProperty("initialSize", "2");
            properties.setProperty("maxActive", "2");
            sourceWithSchema = DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Connection connection = sourceWithSchema.getConnection();
        return connection;
    }

    private static DataSource sourceWithNotRoot;
    public static Connection getDruidDingoConnectionWithNotRoot(String userName, String passwd) throws ClassNotFoundException, SQLException {
        Properties properties = null;
        try {
            properties = new Properties();
            InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream("druid_dingo.properties");
            properties.load(inputStream);
            String defaultConnectIP = CommonArgs.getDefaultDingoClusterIP();
            String port = CommonArgs.getDefaultExecutorPort();
            String jdbcUrl = "jdbc:dingo:thin:url=" + defaultConnectIP + ":" + port + "/" + SCHEMANAME;
            properties.setProperty("url", jdbcUrl);
            properties.setProperty("username", userName);
            properties.setProperty("password", passwd);
            properties.setProperty("initialSize", "2");
            properties.setProperty("maxActive", "2");
            sourceWithNotRoot = DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Connection connection = sourceWithNotRoot.getConnection();
        return connection;
    }

    public static List<String> getSchemaList() throws SQLException {
        Connection connection = getDruidDingoConnection();
        return baseGetSchemaList(connection);
    }

    public static List<String> getTableList() throws SQLException {
        Connection connection = getDruidDingoConnection();
        return baseGetTableList(connection, SCHEMANAME);
    }

    public static List<String> getTableListWithSchema(String schemaName) throws SQLException {
        Connection connection = getDruidDingoConnection();
        return baseGetTableListWithSchema(connection, schemaName);
    }

    public static List<String> getAllTableList() throws SQLException {
        Connection connection = getDruidDingoConnection();
        return baseGetAllTableList(connection);
    }
}

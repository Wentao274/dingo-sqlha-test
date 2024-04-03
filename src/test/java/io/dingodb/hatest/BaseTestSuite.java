package io.dingodb.hatest;

import io.dingodb.DingSDKHelper;
import io.dingodb.DruidUtilsDingo;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import utils.IniReader;

import java.sql.Connection;

public class BaseTestSuite {
    public static IniReader iniReader;
    
    @BeforeSuite (alwaysRun = true, description = "测试开始前的准备工作")
    public static void beforeSuite() {
        System.out.println("测试开始前，验证JDBC连接");
        Connection connection = null;
        try {
            connection = DruidUtilsDingo.getDruidDingoConnection();
            Assert.assertNotNull(connection);
            iniReader = new IniReader("src/test/resources/hatest/ini/dingo.ini");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            DruidUtilsDingo.closeResource(connection, null, null);
        }
        
        String coordinatorCfg = "src/main/resources/conf/client13.yaml";
        MetaServiceClient metaServiceClient = null;
        try {
            metaServiceClient = DingSDKHelper.getMetaClient(coordinatorCfg);
            Assert.assertNotNull(metaServiceClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            DingSDKHelper.metaClientClose(metaServiceClient);
        }
    }

    @AfterSuite(alwaysRun = true, description = "所有测试结束后的操作")
    public static void afterSuite() {
        System.out.println("所有测试结束后，清理测试数据");
    }
}

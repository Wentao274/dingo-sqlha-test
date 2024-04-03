package io.dingodb.hatest;

import datahelper.YamlDataHelper;
import io.dingodb.DingSDKHelper;
import io.dingodb.DingoHelperDruid;
import io.dingodb.common.CommonArgs;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import utils.CastUtils;
import utils.RemoteRunSH;
import utils.ScpClientUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestHA extends BaseTestSuite {
    private static DingoHelperDruid dingoHelperDruid;
    public static MetaServiceClient metaServiceClient;
    private static HashSet<String> createTableSet = new HashSet<>();
    private static HashSet<String> createSchemaSet = new HashSet<>();
    private static final Logger logger = LoggerFactory.getLogger(TestHA.class);

    @BeforeClass(alwaysRun = true)
    public void setupAll() {
        dingoHelperDruid = new DingoHelperDruid();
    }

    @AfterClass(alwaysRun = true)
    public void tearDownAll() throws SQLException {
        logger.info("Create table set: " + createTableSet);
        if(createTableSet.size() > 0) {
            for (String s : createTableSet) {
                dingoHelperDruid.doDropTable(s);
            }
        }

        if(createSchemaSet.size() > 0) {
            System.out.println(createSchemaSet);
            for (String s : createSchemaSet) {
                if (!s.equalsIgnoreCase("DINGO")) {
                    dingoHelperDruid.doDropSchema(s);
                }
            }
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() {
    }

    @AfterMethod(alwaysRun = true)
    public void cleanUp() {
    }
    
    @Test(priority = 0, enabled = true, alwaysRun = true, dataProvider = "HaData1", dataProviderClass = YamlDataHelper.class, description = "测试表和索引数据在多个节点的raft日志提交一致性")
    public void test01RaftCommitIndexConsistent(LinkedHashMap<String,String> param) throws Exception {
        if (param.get("Testable").trim().equals("n") || param.get("Testable").trim().equals("N")) {
            throw new SkipException("skip this test case");
        }
        logger.info("-----------------------------------------------------------------------------------------------");
        logger.info("测试用例ID： " + param.get("TestID"));

        List<String> tableList = new ArrayList<>();
        List<String> createSchemaList = new ArrayList<>();
        if (param.get("Schema").trim().length() > 0) {
            createSchemaList = CastUtils.construct1DListIncludeBlank(param.get("Schema"),",");
            createSchemaSet.addAll(createSchemaList);
            for (int i = 0; i < createSchemaList.size(); i++) {
//                if (!param.get("Schema").equalsIgnoreCase("DINGO")) {
//                    List<String> checkSchemaTableList = DruidUtilsDingo.getTableListWithSchema(createSchemaList.get(i));
//                    for (String t : checkSchemaTableList) {
//                        dingoHelperDruid.doDropTable(createSchemaList.get(i) + "." + t);
//                    }
//                }

                if (!createSchemaList.get(i).equalsIgnoreCase("DINGO")) {
//                    dingoHelperDruid.doDropSchema(createSchemaList.get(i));
                    dingoHelperDruid.execSql("create schema if not exists " + createSchemaList.get(i));
                }
            }
        }

        String tableName = "";
        if (param.get("Table_meta_ref").trim().length() > 0) {
            String tableMeta = param.get("Table_meta_ref").trim();
            if (param.get("Case_table_dependency").trim().length() > 0) {
                tableName = param.get("Case_table_dependency").trim() + "_" + tableMeta;
            } else {
                tableName = param.get("TestID").trim() + "_" + tableMeta;
                dingoHelperDruid.doDropTable(tableName);
                dingoHelperDruid.execFile(TestHA.class.getClassLoader().getResourceAsStream(iniReader.getValue("TableSchema",tableMeta)), tableName);
                tableList.add(tableName);
            }
        }
        createTableSet.addAll(tableList);

        if (param.get("Table_value_ref").trim().length() > 0) {
            String valueTag = param.get("Table_value_ref").trim();
            dingoHelperDruid.execFile(TestHA.class.getClassLoader().getResourceAsStream(iniReader.getValue("TableValue", valueTag)), tableName);
        }

        String coordinatorCfg = param.get("Client_conf");
        String schemaName = param.get("Schema");
        metaServiceClient = DingSDKHelper.getMetaClient(coordinatorCfg);
        List<String> storeNodeList = DingSDKHelper.getStoreNodeList(coordinatorCfg);
        List<String> vectorNodeList = DingSDKHelper.getVectorIndexNodeList(coordinatorCfg);
        checkTableConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);

        if (param.get("Scalar_index").trim().length() > 0) {
            checkScalarIndexConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);
        }

        if (param.get("Vector_index").trim().length() > 0) {
            checkVectorIndexConsistent(coordinatorCfg, schemaName, tableName, vectorNodeList);
        }
        logger.info("表和索引raft验证完毕！");
        
        if (param.get("DML_op").trim().length() > 0) {
            logger.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            logger.info("验证DML操作的raft一致性");
            dingoHelperDruid.execFile(TestHA.class.getClassLoader().getResourceAsStream(iniReader.getValue("dml_op", param.get("DML_op").trim())), tableName);
            
            Thread.sleep(300);
            checkTableConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);
            if (param.get("Scalar_index").trim().length() > 0) {
                checkScalarIndexConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);
            }
            if (param.get("Vector_index").trim().length() > 0) {
                checkVectorIndexConsistent(coordinatorCfg, schemaName, tableName, vectorNodeList);
            }
            logger.info("DML操作raft一致性验证完毕");
        }
        
        if (param.get("Ansible_client").trim().length() > 0) {
            Properties properties = new Properties();
            InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream("conf/ansible.properties");
            properties.load(inputStream);
            String haHost = properties.getProperty(param.get("Ansible_client").trim());
            int sshPort =Integer.parseInt(properties.getProperty("sshport"));
            String scriptTargetDirectory = properties.getProperty(param.get("HA_filetarget"));
            
            if (param.get("Coordinator_stop").trim().length() > 0) {
                String coordinator_stop_shell = "";
                try {
                    String stopCoorNode = param.get("Coordinator_stop").trim();
                    ScpClientUtil scpClient = ScpClientUtil.getInstance(haHost, sshPort, CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass());
                    String filePath = properties.getProperty("stop_coor");
                    scpClient.putFile(filePath, scriptTargetDirectory);
                    Thread.sleep(3000);
                    int lastIndexOfSlash = filePath.lastIndexOf("/");
                    coordinator_stop_shell = filePath.substring(lastIndexOfSlash + 1);
                    logger.info("Stop coordinator shellName: " + coordinator_stop_shell);
//                    RemoteRunSH.cleanShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, coordinator_stop_shell);
                    RemoteRunSH.execShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, coordinator_stop_shell, stopCoorNode);

                    if (param.get("After_coordinator_stop_dml").trim().length() > 0) {
                        logger.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                        logger.info("验证停止coordinator后DML操作的raft一致性");
                        dingoHelperDruid.execFile(TestHA.class.getClassLoader().getResourceAsStream(iniReader.getValue("dml_op", param.get("After_coordinator_stop_dml").trim())), tableName);

                        Thread.sleep(3000);
                        checkTableConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);
                        if (param.get("Scalar_index").trim().length() > 0) {
                            checkScalarIndexConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);
                        }
                        if (param.get("Vector_index").trim().length() > 0) {
                            checkVectorIndexConsistent(coordinatorCfg, schemaName, tableName, vectorNodeList);
                        }
                    }
                } finally {
                    logger.info("coordinator的停止场景的表操作的raft一致性验证完毕,删除脚本");
                    RemoteRunSH.cleanShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, coordinator_stop_shell);
                }
            }
            
            if (param.get("Coordinator_start").trim().length() > 0) {
                String coordinator_start_shell = "";
                try {
                    String startCoorNode = param.get("Coordinator_start").trim();
                    ScpClientUtil scpClient = ScpClientUtil.getInstance(haHost, sshPort, CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass());
                    String filePath = properties.getProperty("start_coor");
                    scpClient.putFile(filePath, scriptTargetDirectory);
                    Thread.sleep(3000);
                    int lastIndexOfSlash = filePath.lastIndexOf("/");
                    coordinator_start_shell = filePath.substring(lastIndexOfSlash + 1);
                    logger.info("Start coordinator shell name: " + coordinator_start_shell);
//                    RemoteRunSH.cleanShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, coordinator_start_shell);
                    RemoteRunSH.execShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, coordinator_start_shell, startCoorNode);

                    if (param.get("After_coordinator_start_dml").trim().length() > 0) {
                        logger.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                        logger.info("验证启动coordinator后DML操作的raft一致性");
                        dingoHelperDruid.execFile(TestHA.class.getClassLoader().getResourceAsStream(iniReader.getValue("dml_op", param.get("After_coordinator_start_dml").trim())), tableName);

                        Thread.sleep(3000);
                        checkTableConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);
                        if (param.get("Scalar_index").trim().length() > 0) {
                            checkScalarIndexConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);
                        }
                        if (param.get("Vector_index").trim().length() > 0) {
                            checkVectorIndexConsistent(coordinatorCfg, schemaName, tableName, vectorNodeList);
                        }
                    }
                } finally {
                    logger.info("coordinator再次启动的场景的表操作的raft一致性验证完毕,删除脚本");
                    RemoteRunSH.cleanShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, coordinator_start_shell);
                }
            }
            
            if (param.get("Store_stop").trim().length() > 0) {
                String store_stop_shell = "";
                try {
                    String stopStoreNode = param.get("Store_stop").trim();
                    ScpClientUtil scpClient = ScpClientUtil.getInstance(haHost, sshPort, CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass());
                    String filePath = properties.getProperty("stop_store");
                    scpClient.putFile(filePath, scriptTargetDirectory);
                    Thread.sleep(3000);
                    int lastIndexOfSlash = filePath.lastIndexOf("/");
                    store_stop_shell = filePath.substring(lastIndexOfSlash + 1);
                    logger.info("Stop store shellName: " + store_stop_shell);
                    RemoteRunSH.execShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, store_stop_shell, stopStoreNode);

                    if (param.get("After_store_stop_dml").trim().length() > 0) {
                        logger.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                        logger.info("验证停止store后DML操作的raft一致性");
                        dingoHelperDruid.execFile(TestHA.class.getClassLoader().getResourceAsStream(iniReader.getValue("dml_op", param.get("After_store_stop_dml").trim())), tableName);

                        Thread.sleep(3000);
                        List<String> activeStoreNodeList = new ArrayList<>(storeNodeList);
                        activeStoreNodeList.removeIf(item -> item.contains(stopStoreNode));
                        System.out.println("存活的store节点： " + activeStoreNodeList);
                        checkTableConsistent(coordinatorCfg, schemaName, tableName, activeStoreNodeList);
                        if (param.get("Scalar_index").trim().length() > 0) {
                            checkScalarIndexConsistent(coordinatorCfg, schemaName, tableName, activeStoreNodeList);
                        }
                        if (param.get("Vector_index").trim().length() > 0) {
                            checkVectorIndexConsistent(coordinatorCfg, schemaName, tableName, vectorNodeList);
                        }
                    }
                } finally {
                    logger.info("store的停止场景的表操作的raft一致性验证完毕,删除脚本");
                    RemoteRunSH.cleanShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, store_stop_shell);
                }
            }

            if (param.get("Store_start").trim().length() > 0) {
                String store_start_shell = "";
                try {
                    String startStoreNode = param.get("Store_start").trim();
                    ScpClientUtil scpClient = ScpClientUtil.getInstance(haHost, sshPort, CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass());
                    String filePath = properties.getProperty("start_store");
                    scpClient.putFile(filePath, scriptTargetDirectory);
                    Thread.sleep(3000);
                    int lastIndexOfSlash = filePath.lastIndexOf("/");
                    store_start_shell = filePath.substring(lastIndexOfSlash + 1);
                    logger.info("Start store shellName: " + store_start_shell);
                    RemoteRunSH.execShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, store_start_shell, startStoreNode);

                    if (param.get("After_store_start_dml").trim().length() > 0) {
                        logger.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                        logger.info("验证启动store后DML操作的raft一致性");
                        dingoHelperDruid.execFile(TestHA.class.getClassLoader().getResourceAsStream(iniReader.getValue("dml_op", param.get("After_store_start_dml").trim())), tableName);

                        Thread.sleep(3000);
                        checkTableConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);
                        if (param.get("Scalar_index").trim().length() > 0) {
                            checkScalarIndexConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);
                        }
                        if (param.get("Vector_index").trim().length() > 0) {
                            checkVectorIndexConsistent(coordinatorCfg, schemaName, tableName, vectorNodeList);
                        }
                    }
                } finally {
                    logger.info("store的启动场景的表操作的raft一致性验证完毕,删除脚本");
                    RemoteRunSH.cleanShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, store_start_shell);
                }
            }

            if (param.get("Index_stop").trim().length() > 0) {
                String index_stop_shell = "";
                try {
                    String stopIndexNode = param.get("Index_stop").trim();
                    ScpClientUtil scpClient = ScpClientUtil.getInstance(haHost, sshPort, CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass());
                    String filePath = properties.getProperty("stop_index");
                    scpClient.putFile(filePath, scriptTargetDirectory);
                    Thread.sleep(3000);
                    int lastIndexOfSlash = filePath.lastIndexOf("/");
                    index_stop_shell = filePath.substring(lastIndexOfSlash + 1);
                    logger.info("Stop index shellName: " + index_stop_shell);
                    RemoteRunSH.execShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, index_stop_shell, stopIndexNode);

                    if (param.get("After_index_stop_dml").trim().length() > 0) {
                        logger.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                        logger.info("验证停止index后DML操作的raft一致性");
                        dingoHelperDruid.execFile(TestHA.class.getClassLoader().getResourceAsStream(iniReader.getValue("dml_op", param.get("After_index_stop_dml").trim())), tableName);

                        Thread.sleep(3000);
                        List<String> activeVectorNodeList = new ArrayList<>(vectorNodeList);
                        activeVectorNodeList.removeIf(item -> item.contains(stopIndexNode));
                        System.out.println("存活的index节点： " + activeVectorNodeList);
                        checkTableConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);
                        if (param.get("Scalar_index").trim().length() > 0) {
                            checkScalarIndexConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);
                        }
                        if (param.get("Vector_index").trim().length() > 0) {
                            checkVectorIndexConsistent(coordinatorCfg, schemaName, tableName, activeVectorNodeList);
                        }
                    }
                } finally {
                    logger.info("index的停止场景的表操作的raft一致性验证完毕,删除脚本");
                    RemoteRunSH.cleanShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, index_stop_shell);
                }
            }

            if (param.get("Index_start").trim().length() > 0) {
                String index_start_shell = "";
                try {
                    String startIndexNode = param.get("Index_start").trim();
                    ScpClientUtil scpClient = ScpClientUtil.getInstance(haHost, sshPort, CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass());
                    String filePath = properties.getProperty("start_index");
                    scpClient.putFile(filePath, scriptTargetDirectory);
                    Thread.sleep(3000);
                    int lastIndexOfSlash = filePath.lastIndexOf("/");
                    index_start_shell = filePath.substring(lastIndexOfSlash + 1);
                    logger.info("Start index shellName: " + index_start_shell);
                    RemoteRunSH.execShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, index_start_shell, startIndexNode);

                    if (param.get("After_index_start_dml").trim().length() > 0) {
                        logger.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                        logger.info("验证启动index后DML操作的raft一致性");
                        dingoHelperDruid.execFile(TestHA.class.getClassLoader().getResourceAsStream(iniReader.getValue("dml_op", param.get("After_index_start_dml").trim())), tableName);

                        Thread.sleep(3000);
                        checkTableConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);
                        if (param.get("Scalar_index").trim().length() > 0) {
                            checkScalarIndexConsistent(coordinatorCfg, schemaName, tableName, storeNodeList);
                        }
                        if (param.get("Vector_index").trim().length() > 0) {
                            checkVectorIndexConsistent(coordinatorCfg, schemaName, tableName, vectorNodeList);
                        }
                    }
                } finally {
                    logger.info("index的启动场景的表操作的raft一致性验证完毕,删除脚本");
                    RemoteRunSH.cleanShell(CommonArgs.getHAConnectUser(), CommonArgs.getHAConnectPass(), haHost, sshPort, scriptTargetDirectory, index_start_shell);
                }
            }
        }

        System.out.println("测试用例" + param.get(""));
        
    }

    private static void checkTableConsistent(String coordinatorCfg, String schemaName, String tableName, List<String> storeNodeList) throws Exception {
        logger.info("验证表的raft已提交日志一致。");
        List<String> tablePartAndRegion = DingSDKHelper.getTablePartAndRegion(coordinatorCfg, schemaName, tableName);
        logger.info("表partition和region: " + tablePartAndRegion);
        List<List<String>> allTableRaftList = getRaftCommitIndexList(storeNodeList, tableName, tablePartAndRegion, "table");
        logger.info("每个节点的table_last_commit_index: " + allTableRaftList);

        boolean tableAreEqual = areAllListsEqual(allTableRaftList);
        Assert.assertTrue(tableAreEqual);
    }
    
    
    private static void checkScalarIndexConsistent(String coordinatorCfg, String schemaName, String tableName, List<String> storeNodeList) throws Exception {
        logger.info("================================================");
        logger.info("验证标量索引的raft已提交日志一致");
        Map<String, List<String>> scalarIndexPartAndRegion = DingSDKHelper.getScalarIndexPartAndRegion(coordinatorCfg, schemaName, tableName);
        logger.info("标量索引partition和region: " + scalarIndexPartAndRegion);
        for (Map.Entry <String, List<String>> entry : scalarIndexPartAndRegion.entrySet()) {
            String scalarIndexName = entry.getKey();
            List<String> itemPartAndRegion = entry.getValue();
            List<List<String>> allScalarItemRaftList = getRaftCommitIndexList(storeNodeList, scalarIndexName, itemPartAndRegion, "index");
            logger.info("标量索引--" + scalarIndexName + "--" + allScalarItemRaftList);
            boolean scalarItemEqual = areAllListsEqual(allScalarItemRaftList);
            Assert.assertTrue(scalarItemEqual);
        }
    }

    private static void checkVectorIndexConsistent(String coordinatorCfg, String schemaName, String tableName, List<String> vectorNodeList) throws Exception {
        logger.info("================================================");
        logger.info("验证向量索引的raft已提交日志一致");
        Map<String, List<String>> vectorIndexPartAndRegion = DingSDKHelper.getVectorIndexPartAndRegion(coordinatorCfg, schemaName, tableName);
        logger.info("向量索引partition和region: " + vectorIndexPartAndRegion);
        for (Map.Entry <String, List<String>> entry : vectorIndexPartAndRegion.entrySet()) {
            String vectorIndexName = entry.getKey();
            List<String> itemPartAndRegion = entry.getValue();
            List<List<String>> allVectorItemRaftList = getRaftCommitIndexList(vectorNodeList, vectorIndexName, itemPartAndRegion, "index");
            logger.info("向量索引--" + vectorIndexName + "--" + allVectorItemRaftList);
            boolean vectorItemEqual = areAllListsEqual(allVectorItemRaftList);
            Assert.assertTrue(vectorItemEqual);
        }
    }
    
    private static List<List<String>> getRaftCommitIndexList(
            List<String> nodeList, 
            String raftName,
            List<String> partAndRegionList,
            String raftType
    ) {
        List<List<String>> allRaftList = new ArrayList<>();
        for (String s : nodeList) {
            logger.info("raft节点:" + s);
            List<String> nodeRaftList = new ArrayList<>();
            for (String pr : partAndRegionList) {
                String curlUrl = "";
                if (raftType.equalsIgnoreCase("table")) {
                    curlUrl = "http://" + s + "/raft_stat/T_2_" + raftName.toUpperCase() + "_part_" + pr;
                } else if (raftType.equalsIgnoreCase("index")) {
                    curlUrl = "http://" + s + "/raft_stat/I_2_" + raftName.toUpperCase() + "_part_" + pr;
                }
                
                String[] curlArray = {"curl", "-XGET", curlUrl, "-H", "Content-Type:application/json", "-d", "JSONString"};
                Process process = null;
                try {
                    process = new ProcessBuilder(curlArray).start();
                    String raftCommit = printStreamString(process); //输出子进程信息
                    process.waitFor(); //等待子进程结束
                    nodeRaftList.add(raftCommit);
                } catch (InterruptedException | IOException e) {
                    process.destroyForcibly();
                }
            }
            logger.info("raft节点" + s + "->" + nodeRaftList);
            allRaftList.add(nodeRaftList);
        }

        return allRaftList;
    }

    private static String printStreamString(Process finalProcess) {
        try {
            InputStreamReader inputStreamReaderINFO = new InputStreamReader(finalProcess.getInputStream());
            BufferedReader bufferedReaderINFO = new BufferedReader(inputStreamReaderINFO);
            String lineStr;
            StringBuilder response = new StringBuilder();
            while ((lineStr = bufferedReaderINFO.readLine()) != null) {
                response.append(lineStr);
                response.append("\n");
            }
            int startIndex = response.indexOf("last_committed_index");
            String targetString = response.toString().substring(startIndex);
            int endIndex = targetString.indexOf("\n");
            String raftCommit = targetString.substring(22, endIndex);
            return raftCommit;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean areAllListsEqual(List<List<String>> listOfLists) {
        // 比较第一个子列表和其他子列表
        List<String> baseList = listOfLists.get(0);
        for (int i = 1; i < listOfLists.size(); i++) {
            if (!baseList.equals(listOfLists.get(i))) {
                return false;
            }
        }
        return true;
    }
}

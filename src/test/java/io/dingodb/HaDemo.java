package io.dingodb;

import io.dingodb.sdk.service.meta.MetaServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class HaDemo {
    public static MetaServiceClient metaServiceClient;
    private static final Logger logger = LoggerFactory.getLogger(HaDemo.class);
    public static void main(String[] args) throws Exception {
        String coordinatorCfg = "src/main/resources/conf/client13.yaml";
        String schemaStr = "DINGO";
        String tableName = "M6";
        String scalarIndexName = "NAME_INDEX";
        String vectorIndexName = "FEATURE_INDEX";
        metaServiceClient = DingSDKHelper.getMetaClient(coordinatorCfg);
        List<String> tablePartAndRegion = DingSDKHelper.getTablePartAndRegion(coordinatorCfg, schemaStr, tableName);
        System.out.println(tablePartAndRegion);
        List<String> nodeList = DingSDKHelper.getStoreNodeList(coordinatorCfg);
//        List<String> nodeList = DingSDKHelper.getIndexNodeList(coordinatorCfg);
//        for (String s : nodeList) {
//            for (String value : tablePartAndRegion) {
//                String curlUrl = "http://" + s + "/raft_stat/T_2_" + tableName + "_part_" + value;
//                System.out.println(curlUrl);
//                String[] curlArray = {"curl", "-XGET", curlUrl, "-H", "Content-Type:application/json", "-d", "JSONString"};
//                Process process = null;
//                try {
//                    process = new ProcessBuilder(curlArray).start();
//                    printStreamString(process); //输出子进程信息
//                    process.waitFor(); //等待子进程结束
//                } catch (InterruptedException | IOException e) {
//                    process.destroyForcibly();
//                }
//            }
//        }
        String value = "60166\\[80115\\]";
        String curlUrl = "http://" + "172.20.3.13:20101" + "/raft_stat/I_2_" + "M6.NAME_INDEX" + "_part_" + value;
        System.out.println(curlUrl);
        String[] curlArray = {"curl", "-XGET", curlUrl, "-H", "Content-Type:application/json", "-d", "JSONString"};
        Process process = null;
        try {
            process = new ProcessBuilder(curlArray).start();
            printStreamString(process); //输出子进程信息
            process.waitFor(); //等待子进程结束
        } catch (InterruptedException | IOException e) {
            process.destroyForcibly();
        }
    }
    
    private static void printStreamString(Process finalProcess) {
        new Thread(() -> {
            try {
                InputStreamReader inputStreamReaderINFO = new InputStreamReader(finalProcess.getInputStream());
                BufferedReader bufferedReaderINFO = new BufferedReader(inputStreamReaderINFO);
                String lineStr;
                StringBuilder response = new StringBuilder();
                while ((lineStr = bufferedReaderINFO.readLine()) != null) {
                    response.append(lineStr);
                    response.append("\n");
                }
                System.out.println(response);
//                int startIndex = response.indexOf("last_committed_index");
//                String targetString = response.toString().substring(startIndex);
//                int endIndex = targetString.indexOf("\n");
//                System.out.println(targetString.substring(22, endIndex + 1));
//                System.out.println("===========================================");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        new Thread( () -> {
            try {
                InputStreamReader inputStreamReaderERROR = new InputStreamReader(finalProcess.getErrorStream());
                BufferedReader bufferedReaderERROR = new BufferedReader(inputStreamReaderERROR);
                String lineStr;
                StringBuilder response = new StringBuilder();
                while ((lineStr = bufferedReaderERROR.readLine()) != null) {
                    response.append(lineStr);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }
}

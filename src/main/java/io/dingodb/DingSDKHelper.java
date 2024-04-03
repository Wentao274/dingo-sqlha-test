package io.dingodb;

import io.dingodb.client.DingoClient;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.index.IndexParameter;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.service.meta.MetaServiceClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class DingSDKHelper {
    public static DingoClient dingoClient;
    public static MetaServiceClient metaServiceClient;
//    static String coordinatorCfg = "src/main/resources/conf/client13.yaml";
    static int retryNum = 100;
    public static DingoClient getDingoClientConn(String coordinatorCfg) throws Exception {
        String coordinatorSvr = getCoorList(coordinatorCfg);
        dingoClient = new DingoClient(coordinatorSvr, retryNum);
        return dingoClient;
    }
    
    public static MetaServiceClient getMetaClient(String coordinatorCfg) throws Exception {
        String coordinatorSvr = getCoorList(coordinatorCfg);
        metaServiceClient = new MetaServiceClient(coordinatorSvr);
        return metaServiceClient;
    }
    
    public static List<String> getTablePartAndRegion(String coordinatorCfg, String schemaName, String tableName) throws Exception {
        metaServiceClient = getMetaClient(coordinatorCfg);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution = metaServiceClient.getSubMetaService(schemaName.toUpperCase()).getRangeDistribution(tableName.toUpperCase());
        List<String> tablePartAndRegion = new ArrayList<>();
        for (Map.Entry <ByteArrayUtils.ComparableByteArray, RangeDistribution> entry : rangeDistribution.entrySet()) {
            String partID = String.valueOf(entry.getValue().getId().parentId());
            String regionID = String.valueOf(entry.getValue().getId().entityId());
            String concatIDs = partID + "\\[" + regionID + "\\]";
            tablePartAndRegion.add(concatIDs);
        }
        return tablePartAndRegion;
    }

    public static Map<String, List<String>> getScalarIndexPartAndRegion(String coordinatorCfg, String schemaName, String tableName) throws Exception {
        return getIndexAndRegionMap(coordinatorCfg, schemaName, tableName, IndexParameter.IndexType.INDEX_TYPE_SCALAR);
    }

    public static Map<String, List<String>> getVectorIndexPartAndRegion(String coordinatorCfg, String schemaName, String tableName) throws Exception {
        return getIndexAndRegionMap(coordinatorCfg, schemaName, tableName, IndexParameter.IndexType.INDEX_TYPE_VECTOR);
    }
    
    private static Map<String, List<String>> getIndexAndRegionMap (String coordinatorCfg, String schemaName, String tableName, IndexParameter.IndexType getType) throws Exception {
        metaServiceClient = getMetaClient(coordinatorCfg);
        Map<DingoCommonId, Table> tableIndexes = metaServiceClient.getSubMetaService(schemaName.toUpperCase()).getTableIndexes(tableName.toUpperCase());
        Map<String, List<String>> indexMap = new HashMap<>();
        for (Map.Entry <DingoCommonId, Table> entry : tableIndexes.entrySet()) {
            List<String> indexPartList = new ArrayList<>();
            String indexName = "";
            final IndexParameter.IndexType indexType = entry.getValue().getIndexParameter().getIndexType();
            if (indexType.equals(getType)) {
                indexName = entry.getValue().getName();
                NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> scalarIndexRangeDistribution = metaServiceClient.getSubMetaService(schemaName.toUpperCase()).getIndexRangeDistribution(entry.getKey());
                for (Map.Entry <ByteArrayUtils.ComparableByteArray, RangeDistribution> scalarEntry : scalarIndexRangeDistribution.entrySet()) {
                    String partID = String.valueOf(scalarEntry.getValue().getId().parentId());
                    String regionID = String.valueOf(scalarEntry.getValue().getId().entityId());
                    String concatIDs = partID + "\\[" + regionID + "\\]";
                    indexPartList.add(concatIDs);
                }
                indexMap.put(indexName, indexPartList);
            }
        }
        return indexMap;
    }
    

    public static List<String> getScalarIndexNameList(String coordinatorCfg, String schemaName, String tableName) throws Exception {
        return getIndexNameList(coordinatorCfg, schemaName, tableName, IndexParameter.IndexType.INDEX_TYPE_SCALAR);
    }

    public static List<String> getVectorIndexNameList(String coordinatorCfg, String schemaName, String tableName) throws Exception {
        return getIndexNameList(coordinatorCfg, schemaName, tableName, IndexParameter.IndexType.INDEX_TYPE_VECTOR);
    }

    public static List<String> getIndexNameList(String coordinatorCfg, String schemaName, String tableName, IndexParameter.IndexType getType) throws Exception {
        metaServiceClient = getMetaClient(coordinatorCfg);
        Map<DingoCommonId, Table> tableIndexes = metaServiceClient.getSubMetaService(schemaName.toUpperCase()).getTableIndexes(tableName.toUpperCase());
        List<String> indexNameList = new ArrayList<>();
        for (Map.Entry <DingoCommonId, Table> entry : tableIndexes.entrySet()) {
            final IndexParameter.IndexType indexType = entry.getValue().getIndexParameter().getIndexType();
            if (indexType.equals(getType)) {
                final String indexName = entry.getValue().getName();
                indexNameList.add(indexName);
            }
        }
        return indexNameList;
    }
    

    public static String getCoorList(String coordinatorCfg) throws Exception {
        DingoConfiguration.parse(coordinatorCfg);
        String coordinatorSvr = DingoConfiguration.instance().find("coordinatorExchangeSvrList", String.class);
        return coordinatorSvr;
    }

    public static List<String> getStoreNodeList(String coordinatorCfg) throws Exception {
        return getNodeList(coordinatorCfg, "20101");
    }

    public static List<String> getVectorIndexNodeList(String coordinatorCfg) throws Exception {
        return getNodeList(coordinatorCfg, "21101");
    }

    public static List<String> getNodeList(String coordinatorCfg, String srvPort) throws Exception {
        DingoConfiguration.parse(coordinatorCfg);
        String coordinatorSvr = DingoConfiguration.instance().find("coordinatorExchangeSvrList", String.class);
        String nodeSvr = coordinatorSvr.replaceAll("22001", srvPort);
        List<String> nodeList = Arrays.asList(nodeSvr.split(","));
        return nodeList;
    }

    
    public static void dingoClientClose(DingoClient dingoClient) {
        try{
            if(dingoClient != null) {
                dingoClient.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void metaClientClose(MetaServiceClient metaServiceClient) {
        try{
            if(metaServiceClient != null) {
                metaServiceClient.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}

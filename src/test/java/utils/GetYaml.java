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

package utils;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GetYaml {
    public static String convertExcelToYaml(String excelFile, int sheetIndex, int cutPathStart) throws IOException, InterruptedException {
        String[] splitTuple = excelFile.split("/");
        String[] nameTuple = splitTuple[splitTuple.length-1].split("\\.");
        String caseGroup = nameTuple[0];
//        System.out.println(caseGroup);

        LinkedHashMap<String, List> map = loadExcelToMap(excelFile,sheetIndex, caseGroup);
//        JSON.toJSONString(map);

        //parseJson
        JsonNode jsonNodeTree = new ObjectMapper().readTree(JSON.toJSONString(map));
        //save it as YAML
        String jsonAsYaml = new YAMLMapper().writeValueAsString(jsonNodeTree);

        Yaml yaml = new Yaml();
        Map<String, Object> mapYaml = (Map<String, Object>) yaml.load(jsonAsYaml);

        DumperOptions dumperOptions = new DumperOptions();
        dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Yaml wyaml = new Yaml(dumperOptions);
        File dir = new File(generateYamlDir(excelFile));
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("创建Yaml文件目录失败！");
            }
        }
        File dumpfile = new File(generateYamlFile(excelFile));
        try (FileWriter writer = new FileWriter(dumpfile)) {
            wyaml.dump(mapYaml, writer);
        } catch (IOException e) {
            throw new RuntimeException("写入Yaml文件失败！");
        }

        String yamlFile = generateYamlFile(excelFile);
        String[] yamlTuple = yamlFile.split("/");
        String[] subYamlTuple = Arrays.copyOfRange(yamlTuple, cutPathStart, yamlTuple.length);
        return String.join("/", subYamlTuple);
    }

    public static String generateYamlFile(String excelFile) {
        String[] splitTuple = excelFile.split("/");
        String[] nameTuple = splitTuple[splitTuple.length-1].split("\\.");
        String caseGroup = nameTuple[0];

        String yamlParentDir = "";
        for (int i=0; i< splitTuple.length - 1; i++) {
            yamlParentDir += splitTuple[i] + "/";
        }
        String yamlDir = yamlParentDir + "yaml";
        String yamlFile = yamlDir + "/" + caseGroup + ".yaml";
        return yamlFile;
    }

    public static String generateYamlDir(String excelFile) {
        String[] splitTuple = excelFile.split("/");
        String[] nameTuple = splitTuple[splitTuple.length-1].split("\\.");
        String caseGroup = nameTuple[0];

        String yamlParentDir = "";
        for (int i=0; i< splitTuple.length - 1; i++) {
            yamlParentDir += splitTuple[i] + "/";
        }
        String yamlDir = yamlParentDir + "yaml";
        return yamlDir;
    }

    public static LinkedHashMap<String, List> loadExcelToMap(String filePath, int sheetIndex, String caseGroup) throws IOException {
        LinkedHashMap<String, List> map = new LinkedHashMap<>();
        List<Map<String, String>> caselist = new ArrayList<>();

        ArrayList<String> arrkey = new ArrayList<>();
        Workbook workbook = ParseExcel.getWorkBook(filePath);
        Sheet sheet = ParseExcel.getSheet(workbook,sheetIndex);
        int rowTotalNum = ParseExcel.getRowNum(sheet);
//        System.out.println("rowTotalNum: " + rowTotalNum);
        int colTotalNum = ParseExcel.getColNum(sheet);
//        System.out.println("columnTotalNum: " + colTotalNum);

        LinkedHashMap<String, String>[] caseMap = new LinkedHashMap[rowTotalNum - 1];
        if (rowTotalNum > 1) {
            for (int i = 0; i < rowTotalNum - 1; i++) {
                caseMap[i] = new LinkedHashMap<>();
            }
        } else {
            System.out.println(filePath + " is empty");
        }

        // 获得首行的列名，作为hashmap的key值
        for (int j = 0; j < colTotalNum; j++) {
            String cellValue = ParseExcel.getCellValue(sheet,0, j);
            arrkey.add(cellValue);
        }

        // 遍历所有的单元格的值添加到hashmap中,将每行的hashmap添加到caseList中
        for (int k = 1; k < rowTotalNum; k++) {
            for (int c = 0; c < colTotalNum; c++) {
                String cellValue = ParseExcel.getCellValue(sheet, k, c);
                caseMap[k - 1].put(arrkey.get(c), cellValue);
            }
            if (!caseMap[k-1].get("Testable").equalsIgnoreCase("n")) {
                caselist.add(caseMap[k-1]);
            }
        }
        workbook.close();
        map.put(caseGroup, caselist);
        return map;
    }
}

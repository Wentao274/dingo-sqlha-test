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

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ParseExcel {
    private String filePath;
    private String sheetName;
    private int sheetIndex;

    public int getSheetIndex() {
        return sheetIndex;
    }

    public void setSheetIndex(int sheetIndex) {
        this.sheetIndex = sheetIndex;
    }

    public ParseExcel(String filePath, int sheetIndex) {
        this.filePath = filePath;
        this.sheetIndex = sheetIndex;
    }

    public ParseExcel(String filePath, String sheetName) {
        this.filePath = filePath;
        this.sheetName = sheetName;
    }

    public ParseExcel() {
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getSheetName() {
        return sheetName;
    }

    public void setSheetName(String sheetName) {
        this.sheetName = sheetName;
    }

    //获取workbook
    public static Workbook getWorkBook(String filePath) {
        Workbook wb = null;
        try{
            if (filePath.endsWith(".xls")) {
                File file = new File(filePath);
                InputStream is = new FileInputStream(file);
                wb = new HSSFWorkbook(is);
                is.close();
            } else if (filePath.endsWith(".xlsx")) {
                wb = new XSSFWorkbook(filePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return wb;
    }

    //获取sheet
    public static Sheet getSheet(Workbook wb, int sheetIndex) {
        return wb.getSheetAt(sheetIndex);
    }

    //获取表格的行数
    public static int getRowNum(Sheet sheet) throws IOException {
        return sheet.getLastRowNum() + 1;
    }

    //获取表格的列数
    public static int getColNum(Sheet sheet) throws IOException {
        Row row = sheet.getRow(0);
        return row.getPhysicalNumberOfCells();
    }

    //获取指定行数据
    public static Row getRowInfo(Sheet sheet, int rowNum) throws IOException {
        return sheet.getRow(rowNum);
    }

    //获取指定单元格数据
    public static String getCellValue(Sheet sheet, int rowNum, int cellNum) throws IOException {
        Cell cell = sheet.getRow(rowNum).getCell(cellNum, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
        String value = getCellStrValue(cell);
        return value;
    }

    public static String getCellStrValue(Cell cell) {
        String value = "";
        switch (cell.getCellType()) {
            case STRING: {
                value = String.valueOf(cell.getRichStringCellValue());
                return value;
            }
            case NUMERIC: {
                value = String.valueOf(cell.getNumericCellValue());
                return value;
            }
            case BOOLEAN: {
                value = String.valueOf(cell.getBooleanCellValue());
                return value;
            }
            case FORMULA: {
                value = String.valueOf(cell.getCellFormula());
                return value;
            }
            case ERROR: {
                value = String.valueOf(cell.getErrorCellValue());
                return value;
            }
            case BLANK: {
                return value;
            }
            default: {
                System.out.println("未知该单元格类型");
                return value;
            }
        }
    }

}

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

package datahelper;

import org.testng.annotations.DataProvider;

import java.io.IOException;

public class YamlDataHelper extends BaseDataHelper{
    @DataProvider (name = "HaData1", parallel = false)
    public Object[][] haCases1() throws IOException, InterruptedException {
        String excelPath = "src/test/resources/hatest/testsuite/sqlha_cases.xlsx";
        String[] excelArray = {excelPath};
        return getEngineCasesData(excelArray);
    }
    
}

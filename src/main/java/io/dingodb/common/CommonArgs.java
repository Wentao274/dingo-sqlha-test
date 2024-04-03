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

import java.text.SimpleDateFormat;
import java.util.Date;

public class CommonArgs {
    public static String getDefaultDingoClusterIP() {
        if (System.getenv().containsKey("ConnectIP")) {
            return System.getenv("ConnectIP");
        }
        return  "172.20.3.13";
    }

    public static String getHAConnectUser() {
        if (System.getenv().containsKey("HA_USER")) {
            return System.getenv("HA_USER");
        }
        return  null;
    }

    public static String getHAConnectPass() {
        if (System.getenv().containsKey("HA_PASS")) {
            return System.getenv("HA_PASS");
        }
        return  null;
    }

    public static String getDefaultConnectUser() {
        if (System.getenv().containsKey("ConnectUser")) {
            return System.getenv("ConnectUser");
        }
        return  null;
    }

    public static String getDefaultConnectPass() {
        if (System.getenv().containsKey("ConnectPass")) {
            return System.getenv("ConnectPass");
        }
        return  null;
    }

    public static String getDefaultExecutorPort() {
        if (System.getenv().containsKey("ExecutorPort")) {
            return System.getenv("ExecutorPort");
        }
        return  "8765";
    }

    public static String getDefaultMySQLPort() {
        if (System.getenv().containsKey("MySQLPort")) {
            return System.getenv("MySQLPort");
        }
        return  "3307";
    }

    //获取dingo-store仓库的最近提交ID
    public static String getStoreCommitID() {
        if (System.getenv().containsKey("COMMIT_ID")) {
            return System.getenv("COMMIT_ID");
        }
        return  "";
    }

    //获取dingo-store仓库的最近提交作者
    public static String getStoreCommitAuthor() {
        if (System.getenv().containsKey("COMMIT_AUTHOR")) {
            return System.getenv("COMMIT_AUTHOR");
        }
        return  "";
    }

    //获取dingo仓库的最近提交ID
    public static String getDingoCommitID() {
        if (System.getenv().containsKey("DINGO_COMMIT_ID")) {
            return System.getenv("DINGO_COMMIT_ID");
        }
        return  "";
    }

    //获取dingo仓库的最近提交作者
    public static String getDingoCommitAuthor() {
        if (System.getenv().containsKey("DINGO_COMMIT_AUTHOR")) {
            return System.getenv("DINGO_COMMIT_AUTHOR");
        }
        return  "";
    }

    //获取代码合并的触发仓库
    public static String getMergeRepo() {
        if (System.getenv().containsKey("MERGE_REPO")) {
            return System.getenv("MERGE_REPO");
        }
        return  "";
    }

    //获取代码合并的时间
    public static String getMergeTime() {
        if (System.getenv().containsKey("MERGE_TIME")) {
            return System.getenv("MERGE_TIME");
        }
        return  "";
    }
    
    //获取触发的分支
    public static String getMergeBranch() {
        if (System.getenv().containsKey("MERGE_BRANCH")) {
            return System.getenv("MERGE_BRANCH");
        }
        return  "";
    }

    //获取构建触发原因
    public static String getBuildCause() {
        if (System.getenv().containsKey("BUILD_CAUSE")) {
            return System.getenv("BUILD_CAUSE");
        }
        return  "";
    }

    //获取当前日期的字符串
    public static String getCurDateStr(String pattern) {
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        String dateNowStr = simpleDateFormat.format(date);
        return dateNowStr;
    }
}

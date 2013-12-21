/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
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
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.util;

public enum StatusCode {
    SUCCESS(0,                    "GRAPHBUILDER: success"),
    BAD_COMMAND_LINE(1,           "GRAPHBUILDER: bad command line"),
    UNABLE_TO_LOAD_INPUT_FILE(2,  "GRAPHBUILDER: unable to load input file"),
    UNHANDLED_IO_EXCEPTION(3,     "GRAPHBUILDER: unhandled IO exception"),
    MISSING_HBASE_TABLE(4,        "GRAPHBUILDER: missing hbase table"),
    HADOOP_REPORTED_ERROR(5,      "GRAPHBUILDER: hadoop reported exception"),
    INTERNAL_PARSER_ERROR(6,      "GRAPHBUILDER: internal parser error"),
    UNABLE_TO_CONNECT_TO_HBASE(7, "GRAPHBUILDER: unable to connect to hbase"),
    CLASS_INSTANTIATION_ERROR(8,  "GRAPHBUILDER: class instantiation error"),
    INDESCRIBABLE_FAILURE(9,      "GRAPHBUILDER: failure"),
    HBASE_ERROR(10,               "GRAPHBUILDER: hbase error"),
    TITAN_ERROR(11,               "GRAPHBUILDER: Titan error"),
    CANNOT_FIND_CONFIG_FILE(12,   "GRAPHBUILDER: cannot locate config file");

    private final int    status;
    private final String message;

    StatusCode(int status, String message) {
        this.status  = status;
        this.message = message;
    }

    public int getStatus(){
        return status;
    }

    public String getMessage() {
        return message;
    }
}

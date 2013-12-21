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
package com.intel.hadoop.graphbuilder.pipeline.output.titan;


import com.intel.hadoop.graphbuilder.util.RuntimeConfig;

import java.util.HashMap;

public class TitanConfig {

    public static final String GB_ID_FOR_TITAN = "_gb_ID";

    private static HashMap<String, String> defaultConfigMap  = new HashMap<>();
    static {
        // Default Titan configuration for Graphbuilder
        defaultConfigMap.put("TITAN_STORAGE_BACKEND",           "hbase");
        defaultConfigMap.put("TITAN_STORAGE_HOSTNAME",          "localhost");
        defaultConfigMap.put("TITAN_STORAGE_TABLENAME",         "titan");
        defaultConfigMap.put("TITAN_STORAGE_PORT",              "2181");
        defaultConfigMap.put("TITAN_STORAGE_CONNECTION-TIMEOUT","10000");
        defaultConfigMap.put("TITAN_STORAGE_BATCH-LOADING",     "true");
        defaultConfigMap.put("TITAN_IDS_BLOCK-SIZE",            "100000");
    }

    public static final RuntimeConfig config = RuntimeConfig
            .getInstanceWithDefaultConfig(defaultConfigMap);
}

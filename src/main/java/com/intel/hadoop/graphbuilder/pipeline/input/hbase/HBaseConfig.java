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
package com.intel.hadoop.graphbuilder.pipeline.input.hbase;

import com.intel.hadoop.graphbuilder.util.RuntimeConfig;

/**
 * This class holds all of the hbase default configs that can later be 
 * overwritten by a config file.
 */
public class HBaseConfig {
    /**
     * Sets Scan objects row caching.
     * @see org.apache.hadoop.hbase.client.Scan
     */
    public static final int    HBASE_CACHE_SIZE            = 500;

    public static final RuntimeConfig config = RuntimeConfig.getInstance
            (HBaseConfig.class);
}

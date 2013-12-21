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

/**
 * This class holds all the command line long option names. Do not reference these directly, 
 * instead reference them through {@code CommonCommandLineOptions}. If you add any new option 
 * names, make sure to also add them to {@code CommonCommandLineOptions}.
 *
 * @see com.intel.hadoop.graphbuilder.util.BaseCLI
 * @see com.intel.hadoop.graphbuilder.util.CommandLineInterface
 */
public class HBaseCommandLineOptions {
    public static final String CMD_EDGES_OPTION_NAME = "edges";
    public static final String CMD_DIRECTED_EDGES_OPTION_NAME = "directedEdges";
    public static final String CMD_TABLE_OPTION_NAME = "tablename";
    public static final String CMD_VERTICES_OPTION_NAME = "vertices";
    public static final String FLATTEN_LISTS_OPTION_NAME = "flattenlists";
    public static final String STRIP_COLUMNFAMILY_NAMES_OPTION_NAME = "stripcolumnfamilynames";
}

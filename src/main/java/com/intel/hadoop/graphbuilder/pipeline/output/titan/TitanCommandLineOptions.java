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

/**
 * Contains the string constants for the Titan graph storage command line options.
 */
public class TitanCommandLineOptions {
    public static final String APPEND = "append";
    public static final String STORE = "titan";
    public static final String OVERWRITE = "overwrite";


    // The constants for the key/index declarations.
    public static final String CMD_KEYS_OPTNAME  = "keys";

    static public final String STRING_DATATYPE = new String("String");
    static public final String FLOAT_DATATYPE  = new String("Float");
    static public final String DOUBLE_DATATYPE = new String("Double");
    static public final String INT_DATATYPE    = new String("Integer");
    static public final String LONG_DATATYPE   = new String("Long");

    static public final String EDGE_INDEXING   = new String("E");
    static public final String VERTEX_INDEXING = new String("V");

    static public final String UNIQUE     = new String("U");
    static public final String NOT_UNIQUE = new String("NU");

    static public final String KEY_DECLARATION_CLI_HELP  =
            "-keys <key rule 1>,<key rule 2>, ... <key rule n>"
                    + " where a key rule is a ; separated list beginning with a column name and including the following "
                    + " options: \n"
                    + STRING_DATATYPE + " selects String datatype for the key's values <default value>\n"
                    + FLOAT_DATATYPE  + " selects Float datatype for the key's values\n"
                    + DOUBLE_DATATYPE + " selects Double datatype for the key's values\n"
                    + INT_DATATYPE    + " selects Integer datatype for the key's values\n"
                    + LONG_DATATYPE   + " selects Long datatype for the key's values\n"
                    + EDGE_INDEXING   + " marks the key to be used as an edge index\n"
                    + VERTEX_INDEXING + " marks the key to be used as a vertex index\n"
                    + UNIQUE          + " marks the key as taking values unique to each vertex\n"
                    + NOT_UNIQUE
                    + " marks the key as taking values not necessarily unique to each vertex <default value>\n ";
}

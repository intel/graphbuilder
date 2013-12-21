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

import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseCommandLineOptions;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanCommandLineOptions;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;


/**
 * A nice wrapper to grab all the commonly used CLI options like input path and output path. Is also a centralized place
 * for the options LongOpt name which gets used by the demo apps, configs, and the tokenizer.
 *
 * <p><b>Usage:</b>
 * <p>
 * <br />
 * BaseCLI.Options.inputPath.get()  Gets the base input option.
 * <br />
 * BaseCLI.Options.inputPath.getLongOpt()  Gets the long option name used to extract values from the CommandLine object.
 * </p>
 * </p>
 *
 * @see CommandLineInterface
 * @see org.apache.commons.cli.CommandLine
 */
public class BaseCLI {

    //hbase command line options
    private static final String CMD_EDGES_OPTION_NAME = HBaseCommandLineOptions.CMD_EDGES_OPTION_NAME;
    private static final String CMD_DIRECTED_EDGES_OPTION_NAME = HBaseCommandLineOptions.CMD_DIRECTED_EDGES_OPTION_NAME;
    private static final String CMD_TABLE_OPTION_NAME = HBaseCommandLineOptions.CMD_TABLE_OPTION_NAME;
    private static final String CMD_VERTICES_OPTION_NAME = HBaseCommandLineOptions.CMD_VERTICES_OPTION_NAME;
    private static final String FLATTEN_LISTS_OPTION_NAME = HBaseCommandLineOptions.FLATTEN_LISTS_OPTION_NAME;
    private static final String STRIP_COLUMNFAMILY_NAMES_OPTION_NAME =
            HBaseCommandLineOptions.STRIP_COLUMNFAMILY_NAMES_OPTION_NAME;

    //titan option long names
    private static final String TITAN_APPEND = TitanCommandLineOptions.APPEND;
    private static final String TITAN_OVERWRITE = TitanCommandLineOptions.OVERWRITE;
    private static final String TITAN_STORAGE = TitanCommandLineOptions.STORE;
    private static final String TITAN_KEY_INDEX_DECLARATION_CLI_HELP = TitanCommandLineOptions.KEY_DECLARATION_CLI_HELP;
    private static final String TITAN_KEY_INDEX = TitanCommandLineOptions.CMD_KEYS_OPTNAME;

    //general options
    private static final String CMD_OUTPUT_OPTION_NAME = "out";
    private static final String CMD_INPUT_OPTION_NAME  = "in";
    private static final String CMD_RDF_NAMESPACE      = "namespace";

    public enum Options{
        hbaseTable(CLI_HBASE_TABLE_NAME_OPTION), vertex(CLI_VERTEX_OPTION), edge(CLI_EDGE_OPTION),
        directedEdge(CLI_DIRECTED_EDGE_OPTION), flattenList(CLI_FLATTEN_LIST_OPTION),
        stripColumnFamilyNames(CLI_STRIP_COLUMNFAMILY_NAMES_OPTION),
        titanAppend(CLI_TITAN_APPEND_OPTION), titanKeyIndex(CLI_TITAN_KEY_INDEX),
        titanOverwrite(CLI_TITAN_OVERWRITE_OPTION),
        titanStorage(CLI_TITAN_STORAGE_OPTION),
        outputPath(CLI_OUTPUT_PATH_OPTION), inputPath(CLI_INPUT_PATH_OPTION);

        private final Option option;
        Options(Option option){this.option = option;}
        public Option get(){return this.option;}
        public String getLongOpt(){return this.option.getLongOpt();}
    }

    //shared options amongst the demo apps no reason duplicate these configs all over the place
    private static final Option CLI_TITAN_STORAGE_OPTION = OptionBuilder.withLongOpt(TITAN_STORAGE)
            .withDescription("select Titan for graph storage")
            .withArgName("titan")
            .create("t");

    private static final Option CLI_FLATTEN_LIST_OPTION = OptionBuilder.withLongOpt(FLATTEN_LISTS_OPTION_NAME)
            .withDescription("Flag that expends lists into multiple items. " )
            .create("F");

    private static final Option CLI_STRIP_COLUMNFAMILY_NAMES_OPTION = OptionBuilder.withLongOpt(STRIP_COLUMNFAMILY_NAMES_OPTION_NAME)
            .withDescription("Flag that strips HBase column family names from the property names used in the graph. " )
            .create("s");

    private static final Option CLI_TITAN_APPEND_OPTION= OptionBuilder.withLongOpt(TITAN_APPEND)
            .withDescription("Append Graph to Current Graph at Specified Titan Table")
            .create("a");

    private static final Option CLI_TITAN_OVERWRITE_OPTION = OptionBuilder.withLongOpt(TITAN_OVERWRITE)
            .withDescription("Overwrite the existing graph at the specified Titan Table")
            .create("O");

    private static final Option CLI_TITAN_KEY_INDEX = OptionBuilder.withLongOpt(TITAN_KEY_INDEX)
            .withDescription("Specify keys, please. " + TITAN_KEY_INDEX_DECLARATION_CLI_HELP)
            .hasArgs()
            .withArgName("Keys")
            .create("k");

    private static final Option CLI_OUTPUT_PATH_OPTION = OptionBuilder.withLongOpt(CMD_OUTPUT_OPTION_NAME)
            .withDescription("output path")
            .hasArg()
            .create("o");

    private static final Option CLI_INPUT_PATH_OPTION = OptionBuilder.withLongOpt(CMD_INPUT_OPTION_NAME)
            .withDescription("input path")
            .hasArg()
            .isRequired()
            .withArgName("input path")
            .create("i");

    private static final Option CLI_HBASE_TABLE_NAME_OPTION = OptionBuilder.withLongOpt(CMD_TABLE_OPTION_NAME)
            .withDescription("HBase table name")
            .hasArgs()
            .isRequired()
            .withArgName("HBase table name")
            .create("t");

    private static final Option CLI_VERTEX_OPTION  = OptionBuilder.withLongOpt(CMD_VERTICES_OPTION_NAME)
            .withDescription("Specify the columns which are vertex tokens and vertex properties" +
                    "Example: --" + CMD_VERTICES_OPTION_NAME + "\"<vertex_col>=[<vertex_prop1>,...]\"")
            .hasArgs()
            .isRequired()
            .withArgName("Vertex-Column-Name")
            .create("v");

    private static final Option CLI_EDGE_OPTION = OptionBuilder.withLongOpt(CMD_EDGES_OPTION_NAME)
            .withDescription("Specify the HTable columns which are undirected edge tokens; " +
                    "Example: --" + CMD_EDGES_OPTION_NAME + "\"<src_vertex_col>,<dest_vertex_col>,<label>,[edge_property_col,...]\"..." +
                    "Note: Edge labels must be unique")
            .hasArgs()
            .withArgName("Edge-Column-Name")
            .create("e");

    private static final Option CLI_DIRECTED_EDGE_OPTION = OptionBuilder.withLongOpt(CMD_DIRECTED_EDGES_OPTION_NAME)
            .withDescription("Specify the columns which are directed edge tokens; " +
                    "Example: --" + CMD_DIRECTED_EDGES_OPTION_NAME + "\"<src_vertex_col>,<dest_vertex_col>,<label>,[edge_property_col,...]\"..." +
                    "Note: Edge labels must be unique")
            .hasArgs()
            .withArgName("Edge-Column-Name")
            .create("d");
}

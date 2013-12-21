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
package com.intel.hadoop.graphbuilder.sampleapplications;

import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanCommandLineOptions;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanOutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseInputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.GraphConstructionPipeline;

import com.intel.hadoop.graphbuilder.util.*;

import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

/**
 * Generates a graph from rows of a big table, store in a graph database.
 * <p>
 *    <ul>
 *        <li>We support only Hbase for the big table.</li>
 *        <li>We support only Titan for the graph database.</li>
 *    </ul>
 * </p>
 *
 * <p>
 *     Path Arguments:
 *     <ul>
 *         <li>The <code>-t</code> option specifies the HBase table from which to read.</li>
 *         <li>The <code>-conf</code> option specifies configuration file.</li>
 *         <li>The <code>-a</code> option tells Titan it can append the newly generated graph to an existing
 *         one in the same table. The default behavior is to abort if you try to use an existing Titan table name.</li>
 *     </ul>
 *     Specify the Titan table name in the configuration file in the property:
 *     <code>graphbuilder.titan.storage_tablename</code>
 * </p>
 *
 * <p>TO SPECIFY EDGES:
 * Specify the edges with a sequence of "edge rules" following the <code>-e</code> flag (for undirected edges) or
 * <code>-d</code> flag (for directed edges). The rules for edge construction are the same for both directed and
 * undirected edges.
 * The first three attributes in the edge rule are the source vertex column, the destination
 * vertex column, and the string label.</p>
 * <code> -e src_col,dest_col>,label,edge_property_col1,...edge_property_coln </code>
 * </p>
 * <p> <code> -d src_col,dest_col>,label,edge_property_col1,...edge_property_coln </code></p>
 * <p>
 * <p>TO SPECIFY VERTICES: 
 * The first attribute in the string is the vertex ID column. Subsequent attributes
 * denote vertex properties and are separated from the first by an equals sign:</p>
 * <code> -v vertex_id_column=vertex_prop1_column,... vertex_propn_column </code>
 * <p>or in the case there are no properties associated with the vertex id:
 * <code> vertex_id_column </code>
 * <p>
 *     The <code>-F</code> option (for "flatten lists") specifies that when a cell containing a JSon list is read 
 *     as a vertex ID, it is to be expanded into one vertex for each entry in the list. This applies to the source 
 *     and destination columns for edges as well. It does not apply to properties.
 * </p>
 * </p>
 *  Because the endpoints of an edge must be vertices, all endpoints of edges are declared to be vertices.
 *  (The declaration is implicit, but the vertices really end up in the graph database.)
 * <p>
 *     EXAMPLES:
 *     <p>
 *<code>-conf /home/user/conf.xml -t my_hbase_table -v "cf:name=cf:age"  -d "cf:name,cf:dept,worksAt,cf:seniority"</code>
 *     </p>
 *     This generates a vertex for each employee annotated by their age, a vertex for each department with at least
 *     one employee, and a directed edge labeled "worksAt" between each employee and their department, annotated by their
 *     seniority in that department.
 * </p>
 *
 * <p>
 *  TO SPECIFY KEYS FOR DATABASE INDICES:
 *  <code>-keys <key rule 1>,<key rule 2>, ... <key rule n></code>
 *  where a key rule is a ; separated list beginning with a column name and including the following options:
 *  <ul>
 *    <li>{@code String} selects the String datatype for the key's values <default value>.</li>
 *    <li>{@code Float} selects the Float datatype for the key's values.</li>
 *    <li>{@code Double} selects the Double datatype for the key's values.</li>
 *    <li>{@code Integer} selects the Integer datatype for the key's values.</li>
 *    <li>{@code Long} selects the Long datatype for the key's value.</li>
 *    <li>{@code E} marks the key to be used as an edge index.</li>
 *    <li>{@code V} marks the key to be used as a vertex index (edge and vertex indexing are not exclusive).</li>
 *     <li>{@code U} marks the key as taking values unique to each vertex.</li>
 *    <li> {@code NU} marks the key as taking values that are not necessarily unique to each vertex.</li>
 *</ul>
 * </p>
 *
 * <p>
 *  EXAMPLE:
 *  <code>-keys cf:name;V;U,cf:tenure:E;V;Integer</code>
 * </p>
 *
 */

public class TableToGraphDB {

    private static final Logger LOG = Logger.getLogger(TableToGraphDB.class);
    private static boolean configFilePresent = false;

    private static CommandLineInterface commandLineInterface = new CommandLineInterface();
    static {
        Options options = new Options();

        options.addOption(BaseCLI.Options.titanAppend.get());

        options.addOption(BaseCLI.Options.titanOverwrite.get());

        options.addOption(BaseCLI.Options.flattenList.get());

        options.addOption(BaseCLI.Options.stripColumnFamilyNames.get());

        options.addOption(BaseCLI.Options.hbaseTable.get());

        options.addOption(BaseCLI.Options.vertex.get());

        options.addOption(BaseCLI.Options.edge.get());

        options.addOption(BaseCLI.Options.directedEdge.get());

        options.addOption(BaseCLI.Options.titanKeyIndex.get());

        commandLineInterface.setOptions(options);
    }

    /**
     * The main method for feature table to graph database construction.
     *
     * @param args Command line arguments.
     */

    public static void main(String[] args)  {

        Timer timer = new Timer();
        configFilePresent = (args[0].equals("-conf"));
        if (!configFilePresent) {
            commandLineInterface.showError("When writing to Titan, the Titan config file must be specified by -conf <config> ");
        }

        CommandLine cmd = commandLineInterface.checkCli(args);


        GraphConstructionPipeline pipeline = new GraphConstructionPipeline();
        commandLineInterface.getRuntimeConfig().addConfig(pipeline);


        String srcTableName = cmd.getOptionValue(BaseCLI.Options.hbaseTable.getLongOpt());

        HBaseInputConfiguration  inputConfiguration  = new HBaseInputConfiguration(srcTableName);

        HBaseGraphBuildingRule buildingRule = new HBaseGraphBuildingRule(cmd);
        buildingRule.setFlattenLists(cmd.hasOption(BaseCLI.Options.flattenList.getLongOpt()));

        TitanOutputConfiguration outputConfiguration = new TitanOutputConfiguration();

        LOG.info("============= Creating graph from feature table ==================");
        timer.start();
        pipeline.run(inputConfiguration, buildingRule,
                GraphConstructionPipeline.BiDirectionalHandling.KEEP_BIDIRECTIONALEDGES,
                outputConfiguration, cmd);
        LOG.info("========== Done creating graph from feature table ================");
        LOG.info("Time elapsed : " + timer.current_time() + " seconds");
    }
}
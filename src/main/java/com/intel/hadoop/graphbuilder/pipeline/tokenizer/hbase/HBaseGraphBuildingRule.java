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
package com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase;

import com.intel.hadoop.graphbuilder.pipeline.input.hbase.GBHTableConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseCommandLineOptions;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.VertexSchema;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class handles the configuration time aspects of the graph construction 
 * rule (graph tokenizer) that converts hbase records into property graph elements.
 * <p>
 * It is responsible for:
 * <ul>
 * <li> Parsing of the graph specification rules; including validation of the rules 
 * and providing static parsing methods for use by the mapper-time graph construction routines.</li>
 * <li> Generating the property graph schema that this graph construction rule generates.</li>
 * <li> At set-up time, populating the configuration with the information required by 
 * the graph construction routine at Map Reduce time.</li>
 * </ul>
 * </p>
 * <p>
 * The rules for specifying a graph are, at present, as follows:
 * </p>
 * <p>
 * <p>EDGES:
 * The first three attributes in the edge string are source vertex column, destination
 * vertex column, and the string label. </p>
 * <code> src_col,dest_col>,label,edge_property_col1,...edge_property_coln </code>
 * </p>
 * <p>
 * <p>VERTICES: 
 * The first attribute in the string is the vertex ID column. Subsequent attributes 
 * denote vertex properties and are separated from the first by an equals sign.</p>
 * <code> vertex_id_column=vertex_prop1_column,... vertex_propn_column </code>
 * <p>Or in the case that there are no properties associated with the vertex id:
 * <code> vertex_id_column </code>
 * </p>
 * <p>
 * <p>EXAMPLE:
 *<code>-conf /home/user/conf.xml -t my_hbase_table -v "cf:name=cf:age" -e "
 cf:name,cf:dept,worksAt,cf:seniority"</code>
 *     <p>
 *     This generates a vertex for each employee annotated by their age, a vertex 
 *     for each department with at least one employee, and an edge labeled "worksAt" 
 *     between each employee and their department, annotated by their seniority in that department.
 * </p>
 *
 * @see GraphBuildingRule
 * @see PropertyGraphSchema
 * @see HBaseTokenizer
 */

public class HBaseGraphBuildingRule implements GraphBuildingRule {

    private static final Logger LOG = Logger.getLogger(HBaseGraphBuildingRule.class);

    private PropertyGraphSchema graphSchema;
    private HBaseUtils hBaseUtils;
    private String srcTableName;
    private String[] vertexRules;
    private String[] edgeRules;
    private String[] directedEdgeRules;
    private boolean  flattenLists          = false;
    private boolean  stripColumnFamilyNames = false;

    private Class vidClass = StringType.class;
    private Class<? extends GraphTokenizer>  tokenizerClass = HBaseTokenizer.class;

    /**
     * Constructs the {@code HBaseGraphBuildingRule} from the command line.
	 * <p>
     * Use the command line to get the hbase table name used as a data source, 
	 * as well as the graph generation rules.
     *
     * @param {@code cmd} The user specified command line.
     */
    public HBaseGraphBuildingRule(CommandLine cmd) {

        this.graphSchema = new PropertyGraphSchema();
        try {
            this.hBaseUtils = HBaseUtils.getInstance();
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNABLE_TO_CONNECT_TO_HBASE,
                    "GRAPHBUILDER_ERROR: Cannot allocate the HBaseUtils object. Check hbase connection.", LOG, e);
        }

        srcTableName = cmd.getOptionValue(BaseCLI.Options.hbaseTable.getLongOpt());

        vertexRules =
                nullsIntoEmptyStringArrays(cmd.getOptionValues(BaseCLI.Options.vertex.getLongOpt()));

        edgeRules =
                nullsIntoEmptyStringArrays(cmd.getOptionValues(BaseCLI.Options.edge.getLongOpt()));

        directedEdgeRules =
                nullsIntoEmptyStringArrays(cmd.getOptionValues(BaseCLI.Options.directedEdge.getLongOpt()));

        this.stripColumnFamilyNames = cmd.hasOption(BaseCLI.Options.stripColumnFamilyNames.getLongOpt());

        checkSyntaxOfVertexRules();
        checkSyntaxOfEdgeRules();

        validateVertexRuleColumnFamilies();
        validateEdgeRuleColumnFamilies();

        generateEdgeSchemata();
        generateVertexSchemata();
    }

    /**
     * Sets the option to flatten lists.
     * <p>When this option is set, string lists serialized as {string1,string2,...stringn} 
     * expand into n different strings string1, ... stringn when used as vertex IDs.</p>
     * @param {@code flattenLists} Boolean.
     */
    public void setFlattenLists(boolean flattenLists) {
        this.flattenLists = flattenLists;
    }

    /**
     * Set the option to strip column family names when creating propery names 
	 * from HBase column names.
     * <p><ul>
	 * <li> when true: {@code cf_name:column_name} is used to populate the 
	 *      property names {@code column_name}.</li>
     * <li> when false: {@code cf_name:column_name} is used to populate the 
	 *      property names {@code cf_name:column_name}.</li> 
	 * </ul></p>
     * @param {@code stripColumnFamilyNames} Boolean.
     */
    public void setStripColumnFamilyNames(boolean stripColumnFamilyNames) {
        this.stripColumnFamilyNames = stripColumnFamilyNames;
    }

    /*
     * A helper function that replaces nulls with empty lists.
     */
    private String[] nullsIntoEmptyStringArrays(String[] in) {
        if (in == null) {
            return new String[0];
        } else {
            return in;
        }
    }

    /*
     * Checks that the vertex rules are syntactically correct.
     * <p>
     * This method does not check if the column names used are present in the hbase table.
     */
    private void checkSyntaxOfVertexRules() {
        return;
    }

    /*
     * Verifies that the edge rules are syntactically correct.
     * <p>
     * This method does not check if the column names are present in the hbase table.
     */
    private void checkSyntaxOfEdgeRules() {


        for (String edgeRule : edgeRules) {
            if (edgeRule.split("\\,").length < 3) {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                        "Edge rule too short; does not specify <source>,<destination>,<label>. Bad edge rule = "
                                + edgeRule, LOG);
            }
        }

        for (String directedEdgeRule : directedEdgeRules) {
            if (directedEdgeRule.split("\\,").length < 3) {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                        "Edge rule too short; does not specify <source>,<destination>,<label>. Bad edge rule = "
                                + directedEdgeRule, LOG);
            }
        }
    }

    /*
     * Checks that the vertex generation rules use only legal column families.
     * <p/>
     * Because hbase allows different rows to contains different columns under each 
     * column family, we cannot validate the full column name against the Hbase table.
     *
     * @return True, if the supplied vertex rules all have column families valid 
	 * for the table.
     */
    private boolean validateVertexRuleColumnFamilies() {

        boolean returnValue = true;

        for (String vertexRule : vertexRules) {

            String vidColumn = HBaseGraphBuildingRule.getVidColNameFromVertexRule(vertexRule);

            returnValue &= hBaseUtils.columnHasValidFamily(vidColumn, srcTableName);

            if (returnValue == false) {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                        "GRAPHBUILDER ERROR: " + vidColumn + " does not belong to a valid column family of table "
                                + srcTableName, LOG);
            }

            String[] vertexPropertiesColumnNames =
                    HBaseGraphBuildingRule.getVertexPropertyColumnsFromVertexRule(vertexRule);

            for (String columnName : vertexPropertiesColumnNames) {
                returnValue &= hBaseUtils.columnHasValidFamily(columnName, srcTableName);
                if (returnValue == false) {
                    GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                            "GRAPHBUILDER ERROR: " + columnName + " does not belong to a valid column family of table "
                                    + srcTableName, LOG);
                }
            }
        }

        return returnValue;
    }

    /*
     * Checks that the edge generation rules use only legal column families.
     * <p>
     * Because hbase allows different rows to contains different columns under 
     * each column family, we cannot validate the full column name against the 
     * Hbase table.
     *
     * @return True, if the supplied edge rules have all column families valid 
	 * for the table.
     */

    private boolean validateEdgeRuleColumnFamilies() {

        boolean returnValue = true;

        for (String edgeRule : edgeRules) {

            String srcVertexColName = HBaseGraphBuildingRule.getSrcColNameFromEdgeRule(edgeRule);
            String tgtVertexColName = HBaseGraphBuildingRule.getDstColNameFromEdgeRule(edgeRule);

            List<String> propertyColNames = HBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(edgeRule);

            returnValue &= hBaseUtils.columnHasValidFamily(srcVertexColName, srcTableName);

            if (returnValue == false) {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                        "GRAPHBUILDER ERROR: " + srcVertexColName + " does not belong to a valid column family of table "
                        + srcTableName, LOG);
            }

            returnValue &= hBaseUtils.columnHasValidFamily(tgtVertexColName, srcTableName);
            if (returnValue == false) {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                        "GRAPHBUILDER ERROR: " + tgtVertexColName + " does not belong to a valid column family of table "
                                + srcTableName, LOG);
            }

            for (String propertyColName : propertyColNames) {
                returnValue &= hBaseUtils.columnHasValidFamily(propertyColName, srcTableName);
                if (returnValue == false) {
                    GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.BAD_COMMAND_LINE,
                            "GRAPHBUILDER ERROR: " + propertyColName + " does not belong to a valid column family of table "
                                    + srcTableName, LOG);
                }
            }
        }

        return returnValue;
    }

    /**
     * Stores the edge and vertex generation rules in the job configuration for 
	 * use by the Map Reduce time graph tokenizer.
     *
     * @param {@code configuration}  A reference to the job configuration in 
	 *                               which the rules for the tokenizer will be stored.
     * @see HBaseTokenizer
     */
    public void updateConfigurationForTokenizer(Configuration configuration) {
        packVertexRulesIntoConfiguration(configuration, vertexRules);
        packEdgeRulesIntoConfiguration(configuration, edgeRules);
        packDirectedEdgeRulesIntoConfiguration(configuration, directedEdgeRules);
        configuration.setBoolean("HBASE_TOKENIZER_FLATTEN_LISTS", flattenLists);
        configuration.setBoolean("HBASE_TOKENIZER_STRIP_COLUMNFAMILY_NAMES", stripColumnFamilyNames);
    }

    /**
     * The class of the Map Reduce-time graph tokenizer.
     *
     * @return  The class of the Map Reduce-time graph tokenizer.
     * @see HBaseTokenizer
     */
    public Class<? extends GraphTokenizer> getGraphTokenizerClass() {
        return this.tokenizerClass;
    }

    /**
     * The class of the vertex ID type used by the graph generated by this rule.
     *
     * @return Class
     */
    public Class vidClass() {
        return this.vidClass;
    }

    /**
     * Gets the schema of the property graphs generated by this graph 
	 * construction rule.
     *
     * @return The schema of the property graphs generated by this graph 
	 * construction rule.
     * @see PropertyGraphSchema
     */
    public PropertyGraphSchema getGraphSchema() {
        return graphSchema;
    }

    /**
     * Extracts the property name from the column name.
     */
    public static String propertyNameFromColumnName(String columnName, boolean stripColumnFamilyNames) {
        if (stripColumnFamilyNames) {
            return columnName.split("\\:")[1];

        } else {
            return columnName;
        }
    }
    private void generateVertexSchemata() {

        for (String vertexRule : vertexRules) {

            VertexSchema vertexSchema = new VertexSchema();

            String[] columnNames = HBaseGraphBuildingRule.getVertexPropertyColumnsFromVertexRule(vertexRule);

            for (String vertexPropertyColumnName : columnNames) {
                String propertyName = propertyNameFromColumnName(vertexPropertyColumnName, stripColumnFamilyNames);
                PropertySchema propertySchema = new PropertySchema(propertyName, String.class);
                vertexSchema.getPropertySchemata().add(propertySchema);
            }

            graphSchema.addVertexSchema(vertexSchema);
        }
    }

    private void generateEdgeSchemata() {

        for (String edgeRule : edgeRules) {

            List<String> columnNames = HBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(edgeRule);
            String label = HBaseGraphBuildingRule.getLabelFromEdgeRule(edgeRule);

            EdgeSchema edgeSchema = new EdgeSchema(label);

            for (String columnName : columnNames) {
                String edgePropertyName = propertyNameFromColumnName(columnName, stripColumnFamilyNames);
                PropertySchema propertySchema = new PropertySchema(edgePropertyName, String.class);
                edgeSchema.getPropertySchemata().add(propertySchema);
            }

            graphSchema.addEdgeSchema(edgeSchema);
        }

        for (String directedEdgeRule : directedEdgeRules) {

            List<String> columnNames = HBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(directedEdgeRule);
            String label = HBaseGraphBuildingRule.getLabelFromEdgeRule(directedEdgeRule);

            EdgeSchema edgeSchema = new EdgeSchema(label);

            for (String columnName : columnNames) {
                String edgePropertyName = propertyNameFromColumnName(columnName, stripColumnFamilyNames);
                PropertySchema propertySchema = new PropertySchema(edgePropertyName, String.class);
                edgeSchema.getPropertySchemata().add(propertySchema);
            }

            graphSchema.addEdgeSchema(edgeSchema);
        }
    }

    public static void packVertexRulesIntoConfiguration(Configuration configuration, String[] vertexRules) {

        String       separator = GBHTableConfiguration.config.getProperty("COL_NAME_SEPARATOR");
        StringBuffer buffer    = new StringBuffer();

        buffer.append(vertexRules[0]);

        for (int i = 1; i < vertexRules.length; i++) {
            buffer.append(separator);
            buffer.append(vertexRules[i]);
        }

        configuration.set(GBHTableConfiguration.config.getProperty("VCN_CONF_NAME"), buffer.toString());
    }

    /**
     * Static helper function that unpacks the vertex rules from the job configuration.
     * <p>
     * Intended to be used by the Map Reduce-time tokenizer.
     *
     * @param {@code configuration}  The job configuration into which the vertex rules 
	 *                               have been stored.
     * @return Array of strings, each encoding a vertex rule.
     * @see HBaseTokenizer
     */
    public static String[] unpackVertexRulesFromConfiguration(Configuration configuration) {
        String separators = "\\" + GBHTableConfiguration.config.getProperty("COL_NAME_SEPARATOR");
        String[] vertexRules = configuration.get(GBHTableConfiguration.config.getProperty("VCN_CONF_NAME")).split(separators);

        return vertexRules;
    }

    public static void packEdgeRulesIntoConfiguration(Configuration configuration, String[] edgeRules) {
        String       separator = GBHTableConfiguration.config.getProperty("COL_NAME_SEPARATOR");
        StringBuffer edgeRuleBuffer    = new StringBuffer();

        if (edgeRules.length > 0) {

            edgeRuleBuffer.append(edgeRules[0]);

            for (int i = 1; i < edgeRules.length; i++) {
                edgeRuleBuffer.append(separator);
                edgeRuleBuffer.append(edgeRules[i]);
            }
        }
        configuration.set(GBHTableConfiguration.config.getProperty("ECN_CONF_NAME"), edgeRuleBuffer.toString());
    }

    public static void packDirectedEdgeRulesIntoConfiguration(Configuration configuration, String[] directedEdgeRules) {

        String       separator = GBHTableConfiguration.config.getProperty("COL_NAME_SEPARATOR");
        StringBuffer directedEdgeRuleBuffer    = new StringBuffer();

        if (directedEdgeRules.length > 0) {

            directedEdgeRuleBuffer.append(directedEdgeRules[0]);

            for (int i = 1; i < directedEdgeRules.length; i++) {
                directedEdgeRuleBuffer.append(separator);
                directedEdgeRuleBuffer.append(directedEdgeRules[i]);
            }
        }

        configuration.set(GBHTableConfiguration.config.getProperty("DECN_CONF_NAME"), directedEdgeRuleBuffer.toString());
    }


    /**
     * Static helper function that unpacks the edge rules from the job configuration.
     * <p>
     * Intended to be used by the Map Reduce-time tokenizer.
     *
     * @param {@code configuration}  The job configuration into which the edge 
	 *                               rules have been stored.
     * @return Array of strings, each encoding a edge rule.
     * @see HBaseTokenizer
     */

    public static String[] unpackEdgeRulesFromConfiguration(Configuration configuration) {
        String separator = "\\" + GBHTableConfiguration.config.getProperty("COL_NAME_SEPARATOR");

        String packedEdgeRules = configuration.get(GBHTableConfiguration.config.getProperty("ECN_CONF_NAME"));

        String[] edgeRules  = null;

        if (packedEdgeRules == null || packedEdgeRules.length() == 0) {
            edgeRules = new String[0];
        } else {
            edgeRules = packedEdgeRules.split(separator);
        }

        return edgeRules;
    }

    /**
     * Static helper function that unpacks the edge rules from the job configuration.
     * <p/>
     * Intended to be used by the MR-time tokenizer.
     *
     * @param {@code configuration} The job configuration into which the edge rules 
	 *                              have been stored.
     * @return Array of strings, each encoding an edge rule.
     * @see HBaseTokenizer
     */

    public static String[] unpackDirectedEdgeRulesFromConfiguration(Configuration configuration) {
        String   separator = "\\" + GBHTableConfiguration.config.getProperty("COL_NAME_SEPARATOR");

        String  packedDirectedEdgeRules = configuration.get(GBHTableConfiguration.config.getProperty("DECN_CONF_NAME"));

        String[] directedEdgeRules  = null;

        if (packedDirectedEdgeRules == null || packedDirectedEdgeRules.length() == 0) {
           directedEdgeRules = new String[0];
        } else {
            directedEdgeRules = packedDirectedEdgeRules.split(separator);
        }

        return directedEdgeRules;
    }

    /**
     * Obtains the vertex ID column name from the vertex rule.
     * <p>
     * <code>[RDF Object],vertex_col1=vertex_prop1,...vertex_coln</code>
     *
     * @return The column name of the vertex ID in the given vertex rule.
     */
    public static String getVidColNameFromVertexRule(String vertexRule) {

        String[] columnNames = vertexRule.split("\\=");
        String vertexIdColumnName = columnNames[0];
        if (vertexIdColumnName.contains(",")) {
            String[] elements  = vertexIdColumnName.split("\\,");
            vertexIdColumnName = elements[1];
        }

        return vertexIdColumnName;
    }

     /**
     * Obtain RDF tag of the vertex from vertex rule.
     * <p/>
     * <code>[RDF Object],vertex_col1=vertex_prop1,...vertex_coln</code>
     *
     * @return the RDF tag of the vertex in the given vertex rule
     */
    public static String getRDFTagFromVertexRule(String vertexRule) {

        String[] columnNames = vertexRule.split("\\=");
        String vertexIdColumnName = columnNames[0];
        if (vertexIdColumnName.contains(",")) {
            String[] elements  = vertexIdColumnName.split("\\,");
            return elements[0];
        } else {
            return null;
        }
    }

    /**
     * Obtains the vertex property column names from the vertex rule.
     * <p>
     * <code>vertex_col1=vertex_prop1,...vertex_coln</code>
     *
     * @return The column names of the properties in the given vertex rule.
     */
    public static String[] getVertexPropertyColumnsFromVertexRule(String vertexRule) {
        String[] vertexPropertyColumns = null;

        if (vertexRule.contains("=")) {
            String[] columnNames = vertexRule.split("\\=");
            vertexPropertyColumns = columnNames[1].split("\\,");
        } else {
            vertexPropertyColumns = new String[0];
        }

        return vertexPropertyColumns;
    }

    /**
     * Obtains the column name of the source vertex ID from an edge rule.
     * <p>
     * The first three attributes in the edge string are the source vertex column, 
     * the destination vertex column, and the string label, as shown in this example.
     * <code>src_vertex_col,dest_vertex_col,label,edge_property_col1,..edge_property_coln</code>
     *
     * @return The full column name of the source vertex ID.
     */

    public static String getSrcColNameFromEdgeRule(String edgeRule) {
        String[] columnNames = edgeRule.split("\\,");
        String srcVertexColName = columnNames[0];

        return srcVertexColName;
    }

    /**
     * Obtains the column name of the destination vertex ID from an edge rule.
     * <p>
     * The first three attributes in the edge string are the source vertex column, 
     * the destination vertex column, and the string label, as shown in this example.
     * <code>src_vertex_col,dest_vertex_col,label,edge_property_col1,..edge_property_coln</code>
     *
     * @return the full column name of the destination vertex ID
     */
    public static String getDstColNameFromEdgeRule(String edgeRule) {
        String[] columnNames = edgeRule.split("\\,");
        String dstVertexColName = columnNames[1];
        return dstVertexColName;
    }

    /**
     * Obtains the edge label from an edge rule.
     * <p>
     * The first three attributes in the edge string are the source vertex column, 
     * the destination vertex column, and the string label, as shown in this example.
     * <code>src_vertex_col,dest_vertex_col,label,edge_property_col1,..edge_property_coln</code>
     *
     * @return The edge label.
     */

    public static String getLabelFromEdgeRule(String edgeRule) {
        String[] columnNames = edgeRule.split("\\,");
        String   label       = columnNames[2];
        return   label;
    }

    /**
     * Obtains the column names for the properties of an edge rule.
     * <p>
     * The first three attributes in the edge string are the source vertex column, 
     * the destination vertex column, and the string label, as shown in this example.
     * <code>src_vertex_col,dest_vertex_col,label,edge_property_col1,..edge_property_coln</code>
     *
     * @return The list of column names for the edge properties.
     */

    public static ArrayList<String> getEdgePropertyColumnNamesFromEdgeRule(String edgeRule) {

        String[] columnNames = edgeRule.split("\\,");

        ArrayList<String> edgePropertyColumnNames = new ArrayList<String>();

        if (columnNames.length >= 3) {
            for (int i = 3; i < columnNames.length; i++) {
                edgePropertyColumnNames.add(columnNames[i]);
            }
        }

        return edgePropertyColumnNames;
    }
}

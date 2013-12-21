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
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.RecordTypeHBaseRow;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.HBaseUtils;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Map Reduce-time routine that creates property graph elements from HBase rows.
 *
 * <p>Its set-up time analog is {@code HBaseGraphBuildingRule}.</p>
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule
 * @see com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseInputConfiguration
 * @see com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseReaderMapper
 */
public class HBaseTokenizer implements GraphTokenizer<RecordTypeHBaseRow, StringType> {

    private static final Logger LOG = Logger.getLogger(HBaseTokenizer.class);

    private List<String>                  vertexIdColumnList;
    private HashMap<String, String[]>     vertexPropColMap;
    private HashMap<String, String>       vertexRDFLabelMap;
    private ArrayList<Vertex<StringType>> vertexList;

    private HashMap<String, EdgeRule>     edgeLabelToEdgeRules;
    private ArrayList<String>             edgeLabelList;
    private ArrayList<Edge<StringType>>   edgeList;

    private boolean                       flattenLists;
    private boolean stripColumnFamilyNames;



    /*
     * Encapsulates the rules for creating edges.
     *
     * <p> Edge rules consist of the following:
     * <ul>
     * <li> A column name from which to read the edge's source vertex.</li>
     * <li> A column name from which to read the edge's destination vertex.</li>
     * <li> A boolean flag denoting if the edge is bidirectional or directed.</li>
     * <li> A list of column names from which to read the edge's properties.</li>
     * </ul></p>
     * <p>Edge rules are indexed by their label, so we do not store the label in the rule.</p>
     */
    private class EdgeRule {
        private String       srcColumnName;
        private String       dstColumnName;
        private List<String> propertyColumnNames;
        boolean              isBiDirectional;

        private EdgeRule() {

        };

        /**
         * This constructor must take source, destination, and bidirectionality 
		 * as arguments.
         * <p>There is no public default constructor.</p>
         * @param {@code srcColumnName}  The column name from which to get the 
		 *                               source vertex.
         * @param {@code dstColumnName}  The column name from which to get the 
		 *                               destination vertex.
         * @param {@code biDirectional}  Is this edge bidirectional or not?
         */
        EdgeRule(String srcColumnName, String dstColumnName, boolean biDirectional) {
            this.srcColumnName       = srcColumnName;
            this.dstColumnName       = dstColumnName;
            this.propertyColumnNames = new ArrayList<String>();
            this.isBiDirectional     = biDirectional;
        }

        String getSrcColumnName() {
            return this.srcColumnName;
        }

        String getDstColumnName() {
            return this.dstColumnName;
        }

        boolean isBiDirectional() {
            return this.isBiDirectional;
        }

        void addPropertyColumnName(String columnName) {
            propertyColumnNames.add(columnName);
        }

        List<String> getPropertyColumnNames() {
            return propertyColumnNames;
        }



    }

    /**
     * Allocates the tokenizer and its constituent collections.
     */

    public HBaseTokenizer() {

        vertexRDFLabelMap  = new HashMap<String, String>();
        vertexPropColMap   = new HashMap<String, String[]>();
        vertexIdColumnList = new ArrayList<String>();
        vertexList         = new ArrayList<Vertex<StringType>>();

        edgeLabelToEdgeRules  = new HashMap<String, EdgeRule>();
        edgeLabelList         = new ArrayList<String>();
        edgeList              = new ArrayList<Edge<StringType>>();
    }

    /**
     * Extracts the vertex and edge generation rules from the configuration.
     *
     * The edge and vertex rules are placed in the configuration by 
	 * the {@code HBaseGraphBuildingRule}.
     *
     * @param {@code conf}  The jobc configuration, provided by Hadoop.
     * @see com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule
     */
    @Override
    public void configure(Configuration conf) {

        this.flattenLists           = conf.getBoolean("HBASE_TOKENIZER_FLATTEN_LISTS",false);
        this.stripColumnFamilyNames = conf.getBoolean("HBASE_TOKENIZER_STRIP_COLUMNFAMILY_NAMES", false);

        // Parse the column names of vertices and properties from command line prompt
        // <vertex_col1>=[<vertex_prop1>,...] [<vertex_col2>=[<vertex_prop1>,...]]

        String[] vertexRules = HBaseGraphBuildingRule.unpackVertexRulesFromConfiguration(conf);

        String   vertexIdColumnName  = null;
        String   vertexRDFLabel      = null;

        for (String vertexRule : vertexRules) {

                vertexIdColumnName = HBaseGraphBuildingRule.getVidColNameFromVertexRule(vertexRule);
                vertexIdColumnList.add(vertexIdColumnName);

                String[] vertexPropertiesColumnNames =
                        HBaseGraphBuildingRule.getVertexPropertyColumnsFromVertexRule(vertexRule);

                vertexPropColMap.put(vertexIdColumnName, vertexPropertiesColumnNames);

                // Vertex RDF labels are maintained in a separate map
                vertexRDFLabel = HBaseGraphBuildingRule.getRDFTagFromVertexRule(vertexRule);
                if (vertexRDFLabel != null) {
                    vertexRDFLabelMap.put(vertexIdColumnName, vertexRDFLabel);
                }
        }

        LOG.info("GRAPHBUILDER_INFO: Number of vertice rules to be read from HBase = " + vertexIdColumnList.size());

        String[] rawEdgeRules         = HBaseGraphBuildingRule.unpackEdgeRulesFromConfiguration(conf);
        String[] rawDirectedEdgeRules = HBaseGraphBuildingRule.unpackDirectedEdgeRulesFromConfiguration(conf);

        final boolean BIDIRECTIONAL = true;
        final boolean DIRECTED      = false;

        for (String rawEdgeRule : rawEdgeRules) {

            String   srcVertexColName     = HBaseGraphBuildingRule.getSrcColNameFromEdgeRule(rawEdgeRule);
            String   tgtVertexColName     = HBaseGraphBuildingRule.getDstColNameFromEdgeRule(rawEdgeRule);
            String   label                = HBaseGraphBuildingRule.getLabelFromEdgeRule(rawEdgeRule);
            List<String> edgePropertyCols =
                    HBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(rawEdgeRule);

           EdgeRule edgeRule = new EdgeRule(srcVertexColName, tgtVertexColName, BIDIRECTIONAL);

            for (String edgePropertyColumn : edgePropertyCols) {
                edgeRule.addPropertyColumnName(edgePropertyColumn);
            }
            edgeLabelToEdgeRules.put(label, edgeRule);
            edgeLabelList.add(label);
        }

        for (String rawDirectedEdgeRule : rawDirectedEdgeRules) {

            String   srcVertexColName     = HBaseGraphBuildingRule.getSrcColNameFromEdgeRule(rawDirectedEdgeRule);
            String   tgtVertexColName     = HBaseGraphBuildingRule.getDstColNameFromEdgeRule(rawDirectedEdgeRule);
            String   label                = HBaseGraphBuildingRule.getLabelFromEdgeRule(rawDirectedEdgeRule);
            List<String> edgePropertyCols =
                    HBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(rawDirectedEdgeRule);

            EdgeRule edgeRule         = new EdgeRule(srcVertexColName, tgtVertexColName, DIRECTED);

            for (String edgePropertyColumn : edgePropertyCols) {
                edgeRule.addPropertyColumnName(edgePropertyColumn);
            }

            edgeLabelToEdgeRules.put(label, edgeRule);
            edgeLabelList.add(label);

        }

    }


    /*
     * Gets the column data from the HBase table. If any errors are encountered, log them.
     *
     * Leading and trailing whitespace is trimmed from all entries.
     *
     * @param {@code columns}         The HTable columns for the current row.
     * @param {@code fullColumnName}  The Name of the HTABLE column - 
	 *                                {@code column_family:column_qualifier}.
     * @param {@code context}         Hadoop's mapper context. Used for error logging.
     */
    private String getColumnData(Result columns, String fullColumnName, Mapper.Context context) {

        String value = Bytes.toString(HBaseUtils.getColumnData(columns, fullColumnName));

        if (null != value) {
            context.getCounter(GBHTableConfiguration.Counters.HTABLE_COLS_READ).increment(1);

            value = value.trim();

            if (value.isEmpty()) {
                context.getCounter(GBHTableConfiguration.Counters.HTABLE_COLS_IGNORED).increment(1l);
                value = null;
            }
        } else {
            context.getCounter(GBHTableConfiguration.Counters.HTABLE_COL_READ_ERROR).increment(1l);
        }

        return value;
    }

    private ArrayList<String> expandString(String string) {

        ArrayList<String> outArray = new ArrayList<String>();

        int inLength = string.length();

        if (this.flattenLists && string.startsWith("{") && string.endsWith("}")) {

            String bracesStrippedString     = string.substring(1,inLength-1);
            String parenthesesDroppedString = bracesStrippedString.replace("(","").replace(")","");
            String[] expandedString         = parenthesesDroppedString.split("\\,");

            for (int i = 0; i < expandedString.length; i++) {
                String trimmedString = expandedString[i].trim();

                if (!trimmedString.isEmpty()) {
                    outArray.add(trimmedString);
                }
            }

        }  else {
            outArray.add(string);
        }

        return outArray;
    }

    /**
     * Reads an hbase record, and generate vertices and edges according to the 
     * generation rules previously extracted from the configuration.
     *
     * @param {@code record}   An hbase row.
     * @param {@code context}  The mapper's context. Used for error logging.
     */

    public void parse(RecordTypeHBaseRow record, Mapper.Context context) {

        ImmutableBytesWritable row     = record.getRow();
        Result                 columns = record.getColumns();

        vertexList.clear();
        edgeList.clear();

        // check row for vertices

        for (String columnName : vertexIdColumnList) {

            String vidCell = getColumnData(columns, columnName, context);

            if (null != vidCell) {
                for (String vertexId : expandString(vidCell)) {

                    // create vertex

                    Vertex<StringType> vertex = new Vertex<StringType>(new StringType(vertexId));

                    // add the vertex properties

                    String[] vpColNames = vertexPropColMap.get(columnName);

                    if (null != vpColNames) {

                        String value = null;

                        if (vpColNames.length > 0) {
                            for (String vertexPropertyColumnName : vpColNames) {
                                value =  getColumnData(columns, vertexPropertyColumnName, context);
                                if (value != null) {
                                    String propName =
                                            HBaseGraphBuildingRule.propertyNameFromColumnName(vertexPropertyColumnName,
                                                    stripColumnFamilyNames);
                                    vertex.setProperty(propName, new StringType(value));
                                }
                            }
                        }
                    }

                    // add the RDF label to the vertex

                    String rdfLabel = vertexRDFLabelMap.get(columnName);
                    if (rdfLabel != null) {
                        vertex.setLabel(new StringType(rdfLabel));
                    }
                    vertexList.add(vertex);
                }
            } else {

                LOG.warn("GRAPHBUILDER_WARN: Null vertex in " + columnName + ", row " + row.toString());
                context.getCounter(GBHTableConfiguration.Counters.HTABLE_COLS_IGNORED).increment(1l);
            }
        } // End of vertex block

        // check row for edges

        String propertyValue;
        String property;
        String srcVertexColName;
        String tgtVertexColName;

        for (String eLabel : edgeLabelList) {

            int          countEdgeAttr  = 0;
            EdgeRule     edgeRule           = edgeLabelToEdgeRules.get(eLabel);
            List<String> edgeAttributeList  = edgeRule.getPropertyColumnNames();
            String[]     edgeAttributes     = edgeAttributeList.toArray(new String[edgeAttributeList.size()]);


            srcVertexColName     = edgeRule.getSrcColumnName();
            tgtVertexColName     = edgeRule.getDstColumnName();

            StringType srcLabel = null;
            String srcLabelString = vertexRDFLabelMap.get(srcVertexColName);
            if (srcLabelString != null) {
                srcLabel = new StringType(srcLabelString);
            }

            StringType tgtLabel = null;
            String tgtLabelString = vertexRDFLabelMap.get(tgtVertexColName);
            if (tgtLabelString != null) {
                tgtLabel = new StringType(tgtLabelString);
            }

            String srcVertexCellString = getColumnData(columns, srcVertexColName, context);
            String tgtVertexCellString = getColumnData(columns, tgtVertexColName, context);

            if (srcVertexCellString != null && tgtVertexCellString != null && eLabel != null) {
                for (String srcVertexName : expandString(srcVertexCellString)) {
                    for (String tgtVertexName: expandString(tgtVertexCellString)) {


                        Edge<StringType> edge = new Edge<StringType>(new StringType(srcVertexName), srcLabel,
                                new StringType(tgtVertexName), tgtLabel, new StringType(eLabel));

                        for (countEdgeAttr = 0; countEdgeAttr < edgeAttributes.length; countEdgeAttr++) {
                            propertyValue = getColumnData(columns, edgeAttributes[countEdgeAttr], context);

                            property =
                                    HBaseGraphBuildingRule.propertyNameFromColumnName(edgeAttributes[countEdgeAttr],
                                            stripColumnFamilyNames);
                            if (property != null) {
                                edge.setProperty(property, new StringType(propertyValue));
                            }
                        }

                        edgeList.add(edge);

                        // need to make sure both ends of the edge are proper vertices!

                        Vertex<StringType> srcVertex = new Vertex<StringType>(new StringType(srcVertexName), srcLabel);
                        Vertex<StringType> tgtVertex = new Vertex<StringType>(new StringType(tgtVertexName), tgtLabel);
                        vertexList.add(srcVertex);
                        vertexList.add(tgtVertex);

                        if (edgeRule.isBiDirectional()) {
                            Edge<StringType> opposingEdge = new Edge<StringType>(new StringType(tgtVertexName),tgtLabel ,
                                                                new StringType(srcVertexName),  srcLabel,
                                                                new StringType(eLabel));

                            // now add the edge properties

                            for (countEdgeAttr = 0; countEdgeAttr < edgeAttributes.length; countEdgeAttr++) {
                                propertyValue = getColumnData(columns, edgeAttributes[countEdgeAttr], context);


                                property =
                                        HBaseGraphBuildingRule.propertyNameFromColumnName(edgeAttributes[countEdgeAttr],
                                                stripColumnFamilyNames);

                                if (property != null) {
                                    opposingEdge.setProperty(property, new StringType(propertyValue));
                                }
                            }
                            edgeList.add(opposingEdge);
                        }
                    }
                }
            } else {

                if (srcVertexCellString == null) {
                    LOG.warn("GRAPHBUILDER_WARN: Null vertex in " + srcVertexColName + ", row " + row.toString());
                    context.getCounter(GBHTableConfiguration.Counters.HTABLE_COLS_IGNORED).increment(1l);
                }

                if (tgtVertexCellString == null) {
                    LOG.warn("GRAPHBUILDER_WARN: Null vertex in " + tgtVertexColName + ", row " + row.toString());
                    context.getCounter(GBHTableConfiguration.Counters.HTABLE_COLS_IGNORED).increment(1l);
                }

                if (eLabel == null) {
                    GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.INTERNAL_PARSER_ERROR,
                            "Null edge label during parsing. Possibly a bad mapper configuration.",LOG);
                }
            }
        }
    }

    /**
     * Obtains the iterator over the vertex list.
     * @return  Iterator over the vertex list.
     */
    public Iterator<Vertex<StringType>> getVertices() {
        return vertexList.iterator();
    }

    /**
     * Obtains the iterator over the edge list.
     * @return Iterator over the edge list.
     */
    @Override
    public Iterator<Edge<StringType>> getEdges() {
        return edgeList.iterator();
    }
}

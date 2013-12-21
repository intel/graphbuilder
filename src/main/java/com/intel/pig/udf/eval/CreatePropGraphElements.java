/* Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder
 */

package com.intel.pig.udf.eval;

import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.types.*;
import com.intel.hadoop.graphbuilder.util.BaseCLI;
import com.intel.hadoop.graphbuilder.util.CommandLineInterface;
import com.intel.pig.data.GBTupleFactory;
import com.intel.pig.data.PropertyGraphElementTuple;
import com.intel.pig.udf.GBUdfException;
import com.intel.pig.udf.GBUdfExceptionHandler;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.WritableComparable;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.TimeUnit;
/**
 * \brief CreatePropGraphElements ... converts tuples of scalar data into bag of property graph elements..
 * <p/>
 *
 * <b>Example of use in pig sript:</b>
 *
 * <pre>
 * {@code
 * REGISTER target/graphbuilder-2.0alpha-with-deps.jar;
 * x = LOAD 'examples/data/employees.csv' USING PigStorage(',') AS (id: int, name: chararray, age: int, dept: chararray, managerId: int, tenure: chararray);
 * DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "name=age,managerId" -e "name,dept,worksAt,tenure"');
 * pge = FOREACH x GENERATE flatten(CreatePropGraphElements(*));
 *    }
 * </pre>
 *
 * The argument to the UDF constructor is a command string interpreted in the following manner:
 * The rules for specifying a graph are, at present, as follows:
 * </p>
 * <p/>
 * <p>
 * <p>EDGES:
 * The first three attributes in the edge string are source vertex field name, destination
 * vertex field name and the string label. Optional property values are listed by the field names by which they
 * are taken.</p>
 * <code> src_fname,dest_fname>,label,edge_property_fname1,...edge_property_fnamen </code>
 * </p>
 * <p>
 * <p>VERTICES: The first attribute in the string is an optional vertex label, the next is the required
 *  ertex ID field name. Subsequent attributes denote vertex properties
 * and are separated from the first by an equals sign:</p>
 * <code> vertex_id_fieldname=vertex_prop1_fieldname,... vertex_propn_fieldname</code>
 * <p>or in the case there are no properties associated with the vertex id:
 * <code> vertex_id_fieldname </code>
 * </p>
 *  * <p>
 *     EXAMPLE:
 *     <p>
 *<code>-v "name=age" -e "name,dept,worksAt,seniority"</code>
 *     </p>
 *     This generates a vertex for each employee annotated by their age, a vertex for each department with at least
 *     one employee, and an edge labeled "worksAt" between each employee and their department, annotated by their
 *     seniority in that department.
 * </p>
 * </p>
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class, duration = 30, timeUnit = TimeUnit.MINUTES)
public class CreatePropGraphElements extends EvalFunc<DataBag> {
    private CommandLineInterface commandLineInterface = new CommandLineInterface();

    private BagFactory mBagFactory = BagFactory.getInstance();

    private String   tokenizationRule;
    private String[] rawEdgeRules;
    private String[] vertexRules;
    private String[] rawDirectedEdgeRules;

    private boolean flattenLists = false;
    private List<String> vertexIdFieldList;
    private Hashtable<String, String[]> vertexPropToFieldNamesMap;
    private Hashtable<String, String> vertexLabelMap;

    private Hashtable<String, EdgeRule>     edgeLabelToEdgeRules;

    private Hashtable<String, Byte> fieldNameToDataType;

    /**
     * Encapsulation of the rules for creating edges.
     *
     * <p> Edge rules consist of the following:
     * <ul>
     * <li> A field name from which to read the edge's source vertex</li>
     * <li> A field name from which to read the edge's destination vertex</li>
     * <li> A boolean flag denoting if the edge is bidirectional or directed</li>
     * <li> A list of field names from which to read the edge's properties</li>
     * </ul></p>
     * <p>Edge rules are indexed by their label, so we do not store the label in the rule.</p>
     */
    private class EdgeRule {
        private String       srcFieldName;
        private String       dstFieldName;
        private List<String> propertyFieldNames;
        boolean              isBiDirectional;

        private EdgeRule() {

        }

        /**
         * Constructor must take source, destination and bidirectionality as arguments.
         * <p>There is no public default constructor.</p>
         * @param srcFieldName  column name from which to get source vertex
         * @param dstFieldName  column name from which to get destination vertex
         * @param biDirectional  is this edge bidirectional or not?
         */
        EdgeRule(String srcFieldName, String dstFieldName, boolean biDirectional) {
            this.srcFieldName = srcFieldName;
            this.dstFieldName = dstFieldName;
            this.propertyFieldNames = new ArrayList<String>();
            this.isBiDirectional     = biDirectional;
        }

        String getSrcFieldName() {
            return this.srcFieldName;
        }

        String getDstFieldName() {
            return this.dstFieldName;
        }

        boolean isBiDirectional() {
            return this.isBiDirectional;
        }

        void addPropertyColumnName(String columnName) {
            propertyFieldNames.add(columnName);
        }

        List<String> getPropertyFieldNames() {
            return propertyFieldNames;
        }
    }

    final boolean BIDIRECTIONAL = true;
    final boolean DIRECTED      = false;

    /*
     * A helper function that replaces nulls with empty lists.
     */
    private String[] nullIntoEmptyArray(String[] in) {
        if (in == null) {
            return new String[0];
        } else {
            return in;
        }
    }

    /**
     * UDF Constructor : uses parse rule in command string to initialize graph construction rules.
     *
     * @param tokenizationRule
     */
    public CreatePropGraphElements(String tokenizationRule) {

        commandLineInterface = new CommandLineInterface();

        Options options = new Options();

        options.addOption(BaseCLI.Options.vertex.get());

        options.addOption(BaseCLI.Options.edge.get());

        options.addOption(BaseCLI.Options.directedEdge.get());
        
        options.addOption(BaseCLI.Options.flattenList.get());
        
        commandLineInterface.setOptions(options);

        CommandLine cmd = commandLineInterface.checkCli(tokenizationRule.split(" "));

        this.tokenizationRule = tokenizationRule;


        vertexLabelMap = new Hashtable<String, String>();
        vertexPropToFieldNamesMap = new Hashtable<String, String[]>();
        vertexIdFieldList = new ArrayList<String>();

        edgeLabelToEdgeRules  = new Hashtable<String, EdgeRule>();

        vertexRules =
                nullIntoEmptyArray(cmd.getOptionValues(BaseCLI.Options.vertex.getLongOpt()));

        rawEdgeRules =
                nullIntoEmptyArray(cmd.getOptionValues(BaseCLI.Options.edge.getLongOpt()));

        rawDirectedEdgeRules =
                nullIntoEmptyArray(cmd.getOptionValues(BaseCLI.Options.directedEdge.getLongOpt()));
        
        flattenLists = cmd.hasOption(BaseCLI.Options.flattenList.getLongOpt());
        
        // Parse the column names of vertices and properties from command line prompt
        // <vertex_col1>=[<vertex_prop1>,...] [<vertex_col2>=[<vertex_prop1>,...]]


        String   vertexIdFieldName  = null;
        String   vertexLabel      = null;

        for (String vertexRule : vertexRules) {

            // this tokenizer is based off the old HBase -> graph tokenizer and uses those parsing/extraction
            // routines as subroutines... those routines have nothing to do with hbase and simply extract field
            // ("column") names from command strings

            vertexIdFieldName = HBaseGraphBuildingRule.getVidColNameFromVertexRule(vertexRule);

            vertexIdFieldList.add(vertexIdFieldName);

            String[] vertexPropertiesFieldNames =
                    HBaseGraphBuildingRule.getVertexPropertyColumnsFromVertexRule(vertexRule);

            vertexPropToFieldNamesMap.put(vertexIdFieldName, vertexPropertiesFieldNames);

            // Vertex labels are maintained in a separate map
            vertexLabel = HBaseGraphBuildingRule.getRDFTagFromVertexRule(vertexRule);
            if (vertexLabel != null) {
                vertexLabelMap.put(vertexIdFieldName, vertexLabel);
            }
        }

        for (String rawEdgeRule : rawEdgeRules) {

            String   srcVertexFieldName     = HBaseGraphBuildingRule.getSrcColNameFromEdgeRule(rawEdgeRule);
            String   tgtVertexFieldName     = HBaseGraphBuildingRule.getDstColNameFromEdgeRule(rawEdgeRule);
            String   label                = HBaseGraphBuildingRule.getLabelFromEdgeRule(rawEdgeRule);
            List<String> edgePropertyFieldNames =
                    HBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(rawEdgeRule);

            EdgeRule edgeRule = new EdgeRule(srcVertexFieldName, tgtVertexFieldName, BIDIRECTIONAL);

            for (String edgePropertyFieldName : edgePropertyFieldNames) {
                edgeRule.addPropertyColumnName(edgePropertyFieldName);
            }
            edgeLabelToEdgeRules.put(label, edgeRule);
        }

        for (String rawDirectedEdgeRule : rawDirectedEdgeRules) {

            String   srcVertexFieldName     = HBaseGraphBuildingRule.getSrcColNameFromEdgeRule(rawDirectedEdgeRule);
            String   tgtVertexFieldName     = HBaseGraphBuildingRule.getDstColNameFromEdgeRule(rawDirectedEdgeRule);
            String   label                = HBaseGraphBuildingRule.getLabelFromEdgeRule(rawDirectedEdgeRule);
            List<String> edgePropertyFieldNames =
                    HBaseGraphBuildingRule.getEdgePropertyColumnNamesFromEdgeRule(rawDirectedEdgeRule);

            EdgeRule edgeRule         = new EdgeRule(srcVertexFieldName, tgtVertexFieldName, DIRECTED);

            for (String edgePropertyFieldName : edgePropertyFieldNames) {
                edgeRule.addPropertyColumnName(edgePropertyFieldName);
            }

            edgeLabelToEdgeRules.put(label, edgeRule);
        }
    }

    private String[] expandString(String string) {

        String[] outArray = null;

        int inLength = string.length();

        if (this.flattenLists && string.startsWith("{") && string.endsWith("}")) {

            String bracesStrippedString     = string.substring(1,inLength-1);
            String parenthesesDroppedString = bracesStrippedString.replace("(","").replace(")","");
            String[] expandedString         = parenthesesDroppedString.split("\\,");
            outArray                        = expandedString;

        }  else {
            outArray = new String[] {string} ;
        }

        return outArray;
    }

    private Object getTupleData(Tuple input, Schema inputSchema, String fieldName) throws IOException{

        int fieldPos = inputSchema.getPosition(fieldName);
        Object output = input.get(fieldPos);

        return  output;
    }

    private void addVertexToPropElementBag(DataBag outputBag, Vertex vertex) throws IOException {

        PropertyGraphElementTuple graphElementTuple = (PropertyGraphElementTuple) new GBTupleFactory()
                .newTuple(1);

        SerializedGraphElementStringTypeVids serializedgraphElement = new SerializedGraphElementStringTypeVids();

        serializedgraphElement.init(vertex);

        try {
            graphElementTuple.set(0, serializedgraphElement);
            outputBag.add(graphElementTuple);
        } catch (ExecException e) {
            warn("Could not set output tuple", PigWarning.UDF_WARNING_1);
            throw new IOException(new GBUdfException(e));
        }
    }

    private void addEdgeToPropElementBag(DataBag outputBag, Edge edge) throws IOException{

        PropertyGraphElementTuple graphElementTuple = (PropertyGraphElementTuple) new GBTupleFactory().newTuple(1);

        SerializedGraphElementStringTypeVids serializedGraphElement = new SerializedGraphElementStringTypeVids();

        serializedGraphElement.init(edge);

        try {
            graphElementTuple.set(0, serializedGraphElement);
            outputBag.add(graphElementTuple);
        } catch (ExecException e) {
            warn("Could not set output tuple", PigWarning.UDF_WARNING_1);
            throw new IOException(new GBUdfException(e));
        }

    }

    private WritableComparable pigTypesToSerializedJavaTypes(Object value, byte typeByte) throws IllegalArgumentException{
        WritableComparable object = null;

        switch(typeByte) {
            case DataType.BYTE:
                object = new IntType((int) value);
                break;
            case DataType.INTEGER:
                object = new IntType((int) value);
                break;
            case DataType.LONG:
                object = new LongType((long) value);
                break;
            case DataType.FLOAT:
                object = new FloatType((float) value);
                break;
            case DataType.DOUBLE:
                object = new DoubleType((double) value);
                break;
            case DataType.CHARARRAY:
                object = new StringType((String) value);
                break;
            default:
                warn("Invalid data type", PigWarning.UDF_WARNING_1);
                throw new IllegalArgumentException();

        }

        return object;
    }

    /**
     * exec - the workhorse for the CreatePropGraphElements UDF
     *
     * Takes a tuple of scalars and outputs a bag of property graph elements.
     *
     * @param input a tuple of scalars
     * @return bag of property graph elements
     * @throws IOException
     */
    @Override
    public DataBag exec(Tuple input) throws IOException {

        Schema inputSchema = getInputSchema();
        fieldNameToDataType = new Hashtable<String,Byte>();

        for (Schema.FieldSchema field : inputSchema.getFields()) {
            fieldNameToDataType.put(field.alias, field.type);
        }

        DataBag outputBag = mBagFactory.newDefaultBag();

        // check tuple for vertices

        for (String fieldName : vertexIdFieldList) {

            String vidCell = (String) getTupleData(input, inputSchema, fieldName);

            if (null != vidCell) {

                for (String vertexId : expandString(vidCell)) {

                    // create vertex

                    Vertex<StringType> vertex = new Vertex<StringType>(new StringType(vertexId));

                    // add the vertex properties

                    String[] vpFieldNames = vertexPropToFieldNamesMap.get(fieldName);

                    if (null != vpFieldNames && vpFieldNames.length > 0) {
                        for (String vertexPropertyFieldName : vpFieldNames) {
                            Object value = null;

                            value =  getTupleData(input, inputSchema, vertexPropertyFieldName);
                            if (value != null) {
                                try {
                                    vertex.setProperty(vertexPropertyFieldName, pigTypesToSerializedJavaTypes(value,
                                            fieldNameToDataType.get(vertexPropertyFieldName)));
                                } catch (ClassCastException e) {
                                    warn("Cannot cast Pig type to Java type, skipping entry.", PigWarning.UDF_WARNING_1);
                                }
                            }
                        }
                    }

                    // add the abel to the vertex

                    String label = vertexLabelMap.get(fieldName);
                    if (label != null) {
                        vertex.setLabel(new StringType(label));
                    }
                    addVertexToPropElementBag(outputBag, vertex);
                }
            }  else {
                warn("Null data, skipping tuple.", PigWarning.UDF_WARNING_1);
            }
        }// End of vertex block

        // check tuple for edges

        Object propertyValue;
        String property;
        String srcVertexFieldName;
        String tgtVertexFieldName;

        for (String eLabel : edgeLabelToEdgeRules.keySet()) {

            int          countEdgeAttr  = 0;
            EdgeRule     edgeRule           = edgeLabelToEdgeRules.get(eLabel);
            List<String> edgeAttributeList  = edgeRule.getPropertyFieldNames();
            String[]     edgeAttributes     = edgeAttributeList.toArray(new String[edgeAttributeList.size()]);


            srcVertexFieldName     = edgeRule.getSrcFieldName();
            tgtVertexFieldName     = edgeRule.getDstFieldName();

            String srcVertexCellString = (String) getTupleData(input, inputSchema, srcVertexFieldName);
            String tgtVertexCellString = (String) getTupleData(input, inputSchema, tgtVertexFieldName);

            StringType srcLabel = null;
            String srcLabelString = vertexLabelMap.get(srcVertexFieldName);
            if (srcLabelString != null) {
                srcLabel = new StringType(srcLabelString);
            }

            StringType tgtLabel = null;
            String tgtLabelString = vertexLabelMap.get(tgtVertexFieldName);
            if (tgtLabelString != null) {
                tgtLabel = new StringType(tgtLabelString);
            }

            if (srcVertexCellString != null && tgtVertexCellString != null && eLabel != null) {

                for (String srcVertexName : expandString(srcVertexCellString)) {
                    for (String tgtVertexName: expandString(tgtVertexCellString)) {

                        Edge<StringType> edge = new Edge<StringType>(new StringType
                                (srcVertexName),srcLabel,
                                new StringType(tgtVertexName),
                                tgtLabel, new StringType(eLabel));

                        for (countEdgeAttr = 0; countEdgeAttr < edgeAttributes.length; countEdgeAttr++) {
                            propertyValue =  getTupleData(input, inputSchema, edgeAttributes[countEdgeAttr]);
                            property = edgeAttributes[countEdgeAttr];

                            if (propertyValue != null) {
                                edge.setProperty(property, pigTypesToSerializedJavaTypes(propertyValue,
                                        fieldNameToDataType.get(edgeAttributes[countEdgeAttr])));
                            }
                        }

                        addEdgeToPropElementBag(outputBag, edge);

                        // need to make sure both ends of the edge are proper
                        // vertices!

                        Vertex<StringType> srcVertex = new Vertex<StringType>(new
                                StringType(srcVertexName), srcLabel);
                        Vertex<StringType> tgtVertex = new Vertex<StringType>(new
                                StringType(tgtVertexName), tgtLabel);
                        addVertexToPropElementBag(outputBag, srcVertex);
                        addVertexToPropElementBag(outputBag, tgtVertex);

                        if (edgeRule.isBiDirectional()) {
                            Edge<StringType> opposingEdge = new Edge<StringType>(new
                                    StringType(tgtVertexName), tgtLabel,
                                    new StringType(srcVertexName), srcLabel,
                                    new StringType(eLabel));

                            // now add the edge properties

                            for (countEdgeAttr = 0; countEdgeAttr < edgeAttributes.length; countEdgeAttr++) {
                                propertyValue = (String) getTupleData(input, inputSchema, edgeAttributes[countEdgeAttr]);

                                property = edgeAttributes[countEdgeAttr];

                                if (propertyValue != null) {
                                    edge.setProperty(property, pigTypesToSerializedJavaTypes(propertyValue,
                                            fieldNameToDataType.get(edgeAttributes[countEdgeAttr])));
                                }
                            }
                            addEdgeToPropElementBag(outputBag, opposingEdge);
                        }
                    }
                }
            }
        }

        return outputBag;
    }

    /**
     * Provide return type information back to the Pig level.
     * @param input
     * @return Schema for a bag of property graph elements packed into unary tuples.
     */
    @Override
    public Schema outputSchema(Schema input) {
        try {

            Schema pgeTuple = new Schema(new Schema.FieldSchema(
                    "property graph element (unary tuple)", DataType.TUPLE));


            return new Schema(new Schema.FieldSchema("property graph elements",
                    pgeTuple, DataType.BAG));

        } catch (FrontendException e) {
            // This should not happen
            throw new RuntimeException("Bug : exception thrown while "
                    + "creating output schema for CreatePropGraphElements udf", e);
        }
    }
}

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
import com.intel.pig.udf.eval.mappings.PropertyGraphMapping;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.InputScanBuffer;

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
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * \brief CreatePropGraphElements ... converts tuples of scalar data into bag of
 * property graph elements..
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
 * The argument to the UDF constructor is a command string interpreted in the
 * following manner: The rules for specifying a graph are, at present, as
 * follows:
 * </p>
 * <p/>
 * <p>
 * <p>
 * EDGES: The first three attributes in the edge string are source vertex field
 * name, destination vertex field name and the string label. Optional property
 * values are listed by the field names by which they are taken.
 * </p>
 * <code> src_fname,dest_fname>,label,edge_property_fname1,...edge_property_fnamen </code>
 * </p>
 * <p>
 * <p>
 * VERTICES: The first attribute in the string is an optional vertex label, the
 * next is the required ertex ID field name. Subsequent attributes denote vertex
 * properties and are separated from the first by an equals sign:
 * </p>
 * <code> vertex_id_fieldname=vertex_prop1_fieldname,... vertex_propn_fieldname</code>
 * <p>
 * or in the case there are no properties associated with the vertex id:
 * <code> vertex_id_fieldname </code>
 * </p>
 * *
 * <p>
 * EXAMPLE:
 * <p>
 * <code>-v "name=age" -e "name,dept,worksAt,seniority"</code>
 * </p>
 * This generates a vertex for each employee annotated by their age, a vertex
 * for each department with at least one employee, and an edge labeled "worksAt"
 * between each employee and their department, annotated by their seniority in
 * that department. </p> </p>
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class, duration = 30, timeUnit = TimeUnit.MINUTES)
public class CreatePropGraphElements extends EvalFunc<DataBag> {
    private BagFactory mBagFactory = BagFactory.getInstance();

    // TODO What does this actually do?
    private boolean flattenLists = false;

    private String[] expandString(String string) {

        String[] outArray = null;

        int inLength = string.length();

        if (this.flattenLists && string.startsWith("{") && string.endsWith("}")) {

            String bracesStrippedString = string.substring(1, inLength - 1);
            String parenthesesDroppedString = bracesStrippedString.replace("(", "").replace(")", "");
            String[] expandedString = parenthesesDroppedString.split("\\,");
            outArray = expandedString;

        } else {
            outArray = new String[] { string };
        }

        return outArray;
    }

    private Object getTupleData(Tuple input, Schema inputSchema, String fieldName) throws IOException {

        int fieldPos = inputSchema.getPosition(fieldName);
        Object output = input.get(fieldPos);

        return output;
    }

    private void addVertexToPropElementBag(DataBag outputBag, Vertex vertex) throws IOException {

        PropertyGraphElementTuple graphElementTuple = (PropertyGraphElementTuple) new GBTupleFactory().newTuple(1);

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

    private void addEdgeToPropElementBag(DataBag outputBag, Edge edge) throws IOException {

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

    private WritableComparable pigTypesToSerializedJavaTypes(Object value, byte typeByte) throws IllegalArgumentException {
        WritableComparable object = null;

        switch (typeByte) {
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

    private static boolean first = true;

    /**
     * exec - the workhorse for the CreatePropGraphElements UDF
     * 
     * Takes a tuple consisting of two fields, the first is a tuple containing
     * the actual data and the second is a map containing the property graph
     * mapping specified per the {@link PropertyGraphMapping} class. Each tuple
     * is converted into a bug of property graph elements.
     * 
     * @param input
     *            Input data
     * @return bag of property graph elements
     * @throws IOException
     */
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() != 1)
            return null;

        Map<String, Integer> fieldMapping = new HashMap<String, Integer>();
        Schema schema = getInputSchema().getField(0).schema;
        if (schema == null)
            return null;

        // Extract field mappings
        for (FieldSchema field : schema.getFields()) {
            if (field.alias == null)
                continue;
            fieldMapping.put(field.alias, schema.getPosition(field.alias));
        }
        if (first) {
            log.info("Field Schema:");
            for (Entry<String, Integer> field : fieldMapping.entrySet()) {
                System.out.println(field.getKey() + " = " + field.getValue());
            }
            first = false;
        }

        // Get the data and the mapping
        Tuple dataTuple = (Tuple) input.get(0);
        PropertyGraphMapping mapping = new PropertyGraphMapping(dataTuple.get(dataTuple.size() - 1));
        DataBag outputBag = mBagFactory.newDefaultBag();
        mapping.apply(dataTuple, fieldMapping, outputBag);
        return outputBag;
    }

    /**
     * Provide return type information back to the Pig level.
     * 
     * @param input
     * @return Schema for a bag of property graph elements packed into unary
     *         tuples.
     */
    @Override
    public Schema outputSchema(Schema input) {
        try {

            Schema pgeTuple = new Schema(new Schema.FieldSchema("property graph element (unary tuple)", DataType.TUPLE));

            return new Schema(new Schema.FieldSchema("property graph elements", pgeTuple, DataType.BAG));

        } catch (FrontendException e) {
            // This should not happen
            throw new RuntimeException(
                    "Bug : exception thrown while " + "creating output schema for CreatePropGraphElements udf", e);
        }
    }
}

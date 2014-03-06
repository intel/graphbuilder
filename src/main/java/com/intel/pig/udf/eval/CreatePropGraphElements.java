/* Copyright (C) 2013 Intel Corporation.
 * Copyright (C) 2014 YarcData LLC
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

import com.intel.pig.udf.GBUdfExceptionHandler;
import com.intel.pig.udf.eval.mappings.PropertyGraphMapping;
import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Converts tuples of scalar data into bag of property graph elements.
 * </p>
 * <p>
 * This UDF takes in a tuple that consists of some data fields and a final field
 * containing a map which describes the property graph mapping. The supported
 * mapping format and options is described in the documentation for
 * {@link PropertyGraphMapping}.
 * </p>
 * 
 * <h4>Example usage</h4>
 * 
 * <pre>
 * REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;
 * x = LOAD 'examples/data/employees.csv' USING PigStorage(',') AS (id: int, name: chararray, age: int, dept: chararray, manager: int, tenure: chararray);
 * 
 * DEFINE CreatePropertyGraph com.intel.pig.udf.eval.CreatePropGraphElements();
 * pge = FOREACH x GENERATE flatten(CreatePropGraphElements(*, [ 'vertices' # [ 'id' # 'id', 'properties' # ( 'name', 'dept', 'age' ) ],
 *                                                               'edges' # [ 'source' # 'id', 'target' # 'manager', 'label' # 'hasManager', 'inverseLabel' # 'manages' ] ]
 *                                                         ));
 * </pre>
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class, duration = 30, timeUnit = TimeUnit.MINUTES)
public class CreatePropGraphElements extends EvalFunc<DataBag> {

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
        // TODO Would be good to cope with the case where we get the tuple
        // directly as well as the case where it gets nested in another tuple as
        // the current code covers

        if (input == null || input.size() == 0)
            return null;

        Map<String, Integer> fieldMapping = new HashMap<String, Integer>();
        Schema schema = null;
        if (input.size() == 1) {
            schema = getInputSchema();
            if (schema != null) {
                FieldSchema fs = schema.getField(0);
                schema = fs != null ? fs.schema : null;
            }
        } else {
            schema = getInputSchema();
        }
        if (schema == null)
            return null;

        // Extract field mappings
        for (FieldSchema field : schema.getFields()) {
            if (field.alias == null)
                continue;
            fieldMapping.put(field.alias, schema.getPosition(field.alias));
        }

        // Get the data and the mapping
        DataBag outputBag = BagFactory.getInstance().newDefaultBag();
        if (input.size() == 1) {
            // Assumes the actual tuple is nested inside this tuple
            Tuple dataTuple = (Tuple) input.get(0);
            PropertyGraphMapping mapping = new PropertyGraphMapping(dataTuple.get(dataTuple.size() - 1));
            mapping.apply(dataTuple, fieldMapping, outputBag);
        } else {
            // Assumes we've received the tuple of data plus mapping directly
            PropertyGraphMapping mapping = new PropertyGraphMapping(input.get(input.size() - 1));
            mapping.apply(input, fieldMapping, outputBag);
        }
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

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

import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.pig.udf.GBUdfExceptionHandler;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Arrays;

/**
 * EdgeList UDF intakes property graph elements and spits out
 * edge list elements which are tuples of (edge source vertex id,
 * edge target vertex id, label), for example,
 * (Employee001, Employee002, worksWith)
 * If the constructor parameter is set to "TRUE", "true" or "1",
 * the edge list element also contains edge properties returned
 * as key value pair, e.g.
 * (Employee001, Employee002, worksWith, workedFor:11yrs)
 *
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class)
public class EdgeList extends EvalFunc<String> {
	private boolean printProperties;
    private static final String[] booleanValues =
            new String [] {"0", "1", "TRUE", "true", "FALSE", "false"};

    /**
     * Constructor of EdgeList
     * @param printProperties Prints edge properties
     *                        if set to "1", "TRUE" or "true",
     *                        does not print edge properties
     *                        if set to "0", "FALSE" or "false"
     */
	public EdgeList(String printProperties) {

        if (!Arrays.asList(booleanValues).contains(printProperties)) {
            throw new IllegalArgumentException(
                    printProperties + " is not a valid argument." +
                    "Use '0', '1', 'TRUE', 'true', 'FALSE' or 'false')");
        }
        if (printProperties.equals("1") ||
            printProperties.equals("TRUE") ||
            printProperties.equals("true")) {
            this.printProperties = true;
        } else if (printProperties.equals("0") ||
                   printProperties.equals("FALSE") ||
                   printProperties.equals("false")) {
            this.printProperties = false;
        }
	}

	@Override
	public String exec(Tuple input) throws IOException {

		SerializedGraphElement serializedGraphElement = (SerializedGraphElement) input.get(0);
        GraphElement graphElement = serializedGraphElement.graphElement();

        if (graphElement == null) {
            warn("Null property graph element", PigWarning.UDF_WARNING_1);
			return null;
        }

		// Print edges, skip vertices
		if (graphElement.isEdge()) {
            String edgeString = "";
            if (this.printProperties) {
                edgeString = graphElement.toString();
            } else {
			    edgeString = graphElement.getSrc().toString() + "\t" +
                             graphElement.getDst().toString() + "\t" +
                             graphElement.getLabel().toString();
            }
			return edgeString;
		}

        return null;
	}

    @Override
    public Schema outputSchema(Schema input) {
        try {
            return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
        } catch (Exception e) {
            throw new RuntimeException("Exception while creating output schema for EdgeList udf", e);
        }
    }
}

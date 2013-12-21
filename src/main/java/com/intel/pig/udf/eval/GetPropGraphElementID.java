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
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.pig.udf.GBUdfExceptionHandler;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import java.io.IOException;

/**
 * \brief GetPropGraphElementID ...returns string objectID of its property graph element.
 *
 * Input: A singleton tuple containing a {@code SerializedGraphElement}
 * Output: The {@code String} ID of the {@code SerializedGraphElement}.
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class)
public class GetPropGraphElementID extends EvalFunc<String>  {

    /**
     * Get the string representation of the ID of a property graph element
     *
     * @param input tuple containing a property graph element
     * @return string objectID of its property graph element
     * @throws IOException
     */
    @Override
    public String exec(Tuple input) throws IOException {

        SerializedGraphElement serializedGraphElement = (SerializedGraphElement) input.get(0);

        GraphElement graphElement = serializedGraphElement.graphElement();

        if (graphElement == null) {
            warn("Null property graph element", PigWarning.UDF_WARNING_1);
			return null;
        }

        return (graphElement.isVertex()) ?
                ("VERTEX " + ((Vertex) graphElement).getId().getName().toString()) :
                ("EDGE " + graphElement.getId().toString());

    }
    
	/**
	 * GetPropGraphElementID UDF returns a chararray
	 */
	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}

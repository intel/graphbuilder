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

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.pig.data.PropertyGraphElementTuple;
import com.intel.pig.udf.GBUdfExceptionHandler;
import com.intel.pig.udf.eval.mappings.RdfMapping;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import java.io.IOException;

/**
 * <p>
 * RDF UDF converts a given {@link PropertyGraphElementTuple} to a bag of RDF
 * statements.
 * </p>
 * <p>
 * If the {@link SerializedGraphElement} is null, this UDF returns null.
 * </p>
 * <p>
 * See <strong>examples/rdf_example.pig</strong> for a usage example
 * </p>
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class)
public class RDF extends EvalFunc<DataBag> {

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() != 1)
            return null;

        // Get the data and the mapping
        Tuple dataTuple = (Tuple) input.get(0);
        RdfMapping mapping = new RdfMapping(dataTuple.get(dataTuple.size() - 1));
        DataBag output = BagFactory.getInstance().newDefaultBag();
        mapping.apply(dataTuple, output);
        return output;
    }

    /**
     * RDF UDF returns a bag of RDF statements.
     */
    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema rdfStatementTuple = new Schema(new Schema.FieldSchema("rdf_statement", DataType.CHARARRAY));
            return new Schema(new FieldSchema("rdf_statements", rdfStatementTuple, DataType.BAG));
        } catch (FrontendException e) {
            throw new RuntimeException("Exception while " + "creating output schema for RDF udf", e);
        }
    }
}

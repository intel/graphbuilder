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

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.intel.pig.udf.GBUdfExceptionHandler;

/**
 * \brief FlattenAsGBString UDF converts a given bag of strings to a flattened
 * representation that GB can process.
 * 
 * <p/>
 * The output will be in the format {str1, str2, ..., strN} <br/>
 * </pre>
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class, duration = 30, timeUnit = TimeUnit.MINUTES)
public class FlattenAsGBString extends EvalFunc<String> {

	/* GB accepts comma delimited fields */
	private String delimeter = ",";

	@Override
	public String exec(Tuple input) throws IOException {
		DataBag bagOfElements = (DataBag) input.get(0);

		if (bagOfElements == null)
			return null;

		StringBuffer flattened = new StringBuffer();
		flattened.append("{");

		long tupleCount = bagOfElements.size();
		int processed = 0;

		Iterator<Tuple> iter = bagOfElements.iterator();
		while (iter.hasNext()) {
			Tuple t = iter.next();
			flattened.append(t.get(0).toString());
			processed++;
			if (processed != tupleCount) {
				flattened.append(delimeter);
			}
		}
		flattened.append("}");

		return flattened.toString();
	}

	/**
	 * FlattenAsGBString UDF returns a flattened Graph Builder string
	 * representation of a bag
	 */
	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}

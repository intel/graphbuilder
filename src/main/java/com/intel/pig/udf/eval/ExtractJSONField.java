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

import com.intel.pig.udf.GBUdfException;
import com.intel.pig.udf.GBUdfExceptionHandler;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.jayway.restassured.path.json.JsonPath.with;

/**
 * \brief ExtractJSONField UDF extracts fields from (potentially complex &
 * nested) JSON data with JSONPath expressions.
 * 
 * ExtractJSONField provides another convenient way of processing complex JSON
 * data with JSONPath expressions using the JSONPath implementation of the <a
 * href="https://code.google.com/p/rest-assured/">RestAssured</a> project. <br/>
 * 
 * <b>Example:</b>
 * <p/>
 * Assume that "tshirts.json" file has a record: <br/>
 * { "Name": "T-Shirt 1", "Sizes": [ { "Size": "Large", "Price": 20.50 }, {
 * "Size": "Medium", "Price": 10.00 } ], "Colors": [ "Red", "Green", "Blue" ]} <br/>
 * <br/>
 * Then here is the corresponding Pig script:
 * 
 * <pre>
 * {@code
       json_data = LOAD 'examples/data/tshirts.json' USING TextLoader() AS (json: chararray);
       extracted_first_tshirts_price = FOREACH json_data GENERATE *, ExtractJSONField(json, 'Sizes[0].Price') AS price: double;
       }
 * </pre>
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class, duration = 30, timeUnit = TimeUnit.MINUTES)
public class ExtractJSONField extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0) {
			warn("Input tuple is null or empty", PigWarning.UDF_WARNING_1);
			return null;
		}

		String inString = (String) input.get(0);
		String query = (String) input.get(1);

		Object queryResult = null;

		try {
			queryResult = with(inString).get(query);
		} catch (IllegalArgumentException e) {
			warn("Failed to process input; error - " + e.getMessage(),
					PigWarning.UDF_WARNING_1);
			return null;
		}

		if (queryResult == null) {
			return null;
		} else if (queryResult instanceof List) {
			List result = (List) queryResult;
			/*
			 * restrict the query expression to return a single primitive value
			 */
			if (result.size() == 1) {
				Object o = result.get(0);
				return String.valueOf(o);
			} else {
				String err = "The query returned multiple results, it has to return a single value.";
				warn(err, PigWarning.UDF_WARNING_1);
				throw new IOException(new GBUdfException(err));
			}
		} else {
			/* for other data types try to convert the output to String */
			String result = "";
			try {
				result = String.valueOf(queryResult);
			} catch (Throwable t) {
				warn("Error converting query output to String.", PigWarning.UDF_WARNING_1);
			}
			return result;
		}
	}

	/**
	 * ExtractJSONField UDF returns an extracted JSON field, which is of type
	 * chararray
	 */
	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}

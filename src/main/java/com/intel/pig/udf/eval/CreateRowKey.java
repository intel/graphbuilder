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
import org.apache.pig.backend.hadoop.hbase.HBaseStorage;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

/**
 * \brief CreateRowKey assigns (prepends) a row key to a given tuple to be used
 * by {@link HBaseStorage}
 * 
 * <p/>
 * Row keys should be designed carefully as they may have significant impact on
 * read/write performance. This UDF only provides a generic randomized row key
 * assignment logic, which may not be suitable for every situation. To
 * understand the implications of row key design please see <a
 * href="http://hbase.apache.org/book/rowkey.design.html">HBase Reference
 * Guide</a>
 * 
 * <b>Example:</b>
 * 
 * <pre>
 * {@code
         x = LOAD 'examples/data/employees.csv' USING PigStorage(',') as (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, underManager:chararray);
         x = FILTER x by id!='';
         keyed_x = FOREACH x GENERATE FLATTEN(CreateRowKey(*));
         STORE keyed_x INTO 'hbase://gb_input_table' 
          		USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf:id cf:name cf:age cf:dept cf:manager cf:underManager');
       }
 * </pre>
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class, duration = 30, timeUnit = TimeUnit.MINUTES)
public class CreateRowKey extends EvalFunc<Tuple> {

	private static TupleFactory tupleFactory = TupleFactory.getInstance();

	@Override
	public Tuple exec(Tuple t) throws IOException {
		int nElements = t.size();
		
		/* we will add a row key */
		Tuple new_tuple = tupleFactory.newTuple(nElements + 1);

		String rowKey;
		try {
			rowKey = generateRowKey(t);
		} catch (NoSuchAlgorithmException e) {
			warn("MD5 algorithm is not available", PigWarning.UDF_WARNING_1);
			throw new IOException(new GBUdfException(
					"MD5 algorithm is not available"));
		}

		/*
		 * first set the generated row key as the first element
		 */
		new_tuple.set(0, rowKey);

		/* now copy the rest of the input tuple t's elements to new_tuple */
		for (int i = 1; i <= nElements; i++) {
			new_tuple.set(i, t.get(i - 1));
		}

		return new_tuple;
	}

	/**
	 * This method generates a row key by getting an MD5 hash of all tuple
	 * elements concatenated as strings. Note that row key generation may have
	 * significant impact on performance depending on the data access patterns.
	 * So, please see <a href="http://hbase.apache.org/book/rowkey.design.html">
	 * HBase Reference Guide</a> for a better understanding of this impact.
	 */
	private String generateRowKey(Tuple t) throws NoSuchAlgorithmException {
		StringBuffer buffer = new StringBuffer();

		/* concat all tuple elements as String */
		for (Object element : t.getAll()) {
			if (element == null) {
				continue;
			}
			buffer.append(element.toString());
		}

		MessageDigest m = MessageDigest.getInstance("MD5");
		m.reset();
		m.update(buffer.toString().getBytes());
		BigInteger bigInt = new BigInteger(1, m.digest());
		return bigInt.toString(16);
	}

	@Override
	public Schema outputSchema(Schema input) {
		try {
			Schema outputSchema = new Schema();

			int nInFields = input.getFields().size();

			outputSchema.add(new FieldSchema("row_key", DataType.CHARARRAY));

			/* add the elements in the input schema */
			for (int i = 0; i < nInFields; i++) {
				outputSchema.add(input.getField(i));
			}

			return new Schema(new Schema.FieldSchema(null, outputSchema,
					DataType.TUPLE));
		} catch (Exception e) {
			throw new RuntimeException("Exception while "
					+ "creating output schema for CreateRowKey udf", e);
		}
	}
}
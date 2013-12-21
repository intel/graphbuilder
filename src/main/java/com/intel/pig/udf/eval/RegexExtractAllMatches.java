/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.builtin.REGEX_EXTRACT_ALL;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.intel.pig.udf.GBUdfException;
import com.intel.pig.udf.GBUdfExceptionHandler;

/**
 * \brief RegexExtractAllMatches returns a bag of all matched strings given a
 * string and a regular expression. <br/>
 * This implementation is based on Pig's built in {@link REGEX_EXTRACT_ALL} UDF.
 * The first element of the given tuple is the source string and the second
 * element is the regular expression. RegexExtractAllMatches UDF only captures
 * the <b>first</b> group specified in the regular expression.
 */
@MonitoredUDF(errorCallback = GBUdfExceptionHandler.class, duration = 30, timeUnit = TimeUnit.MINUTES)
public class RegexExtractAllMatches extends EvalFunc<DataBag> {
	private static TupleFactory tupleFactory = TupleFactory.getInstance();

	private String regularExpression = null;

	@Override
	public DataBag exec(Tuple input) throws IOException {
		Pattern pattern = null;
		if (input.size() != 2) {
			String msg = "Only 2 parameters are allowed. Must provide string source and regular expression.";
			throw new IOException(new GBUdfException(msg));
		}

		String sourceString = (String) input.get(0);
		if (sourceString == null || sourceString.isEmpty())
			return null;

		Matcher m = null;

		try {
			regularExpression = (String) input.get(1);
			pattern = Pattern.compile(regularExpression);
			m = pattern.matcher(sourceString);
		} catch (PatternSyntaxException e) {
			String msg = "Invalid regular expression: " + input.get(1);
			throw new IOException(new GBUdfException(msg, e));
		} catch (NullPointerException e) {
			String msg = "Regular expression is null";
			throw new IOException(new GBUdfException(msg));
		} catch (Throwable t) {
			warn("Match failed", PigWarning.UDF_WARNING_1);
			return null;
		}

		DataBag result = DefaultBagFactory.getInstance().newDefaultBag();

		while (m.find()) {
			Tuple matchedString = tupleFactory.newTuple(1);
			matchedString.set(0, m.group(1));
			result.add(matchedString);
		}

		return result;
	}

	/**
	 * RegexExtractAllMatches UDF returns a bag of extracted strings from the
	 * source str.
	 */
	@Override
	public Schema outputSchema(Schema input) {
		try {
			Schema innerSchema = new Schema(new Schema.FieldSchema(null,
					DataType.CHARARRAY));
			return new Schema(new FieldSchema(null, innerSchema, DataType.BAG));
		} catch (FrontendException e) {
			throw new RuntimeException("Exception while "
					+ "creating output schema for RDF udf", e);
		}
	}
}

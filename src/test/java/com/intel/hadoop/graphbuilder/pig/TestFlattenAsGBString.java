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
package com.intel.hadoop.graphbuilder.pig;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFlattenAsGBString {
	EvalFunc<?> flattenUdf;
	TupleFactory tupleFactory = TupleFactory.getInstance();

	@Before
	public void setup() throws Exception {
		System.out.println("*** Starting FlattenAsGBString tests. ***");
		flattenUdf = (EvalFunc<?>) PigContext
				.instantiateFuncFromSpec(
                        "com.intel.pig.udf.eval.FlattenAsGBString()");
	}

	@Test
	public void runTests() throws IOException {
		DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();
		String result = null;

		/* check with null input */
		Tuple t = tupleFactory.newTuple(1);
		t.set(0, null);
		result = (String) flattenUdf.exec(t);
		assertEquals("Flatten test failed", result, null);

		/* check with no elements */
		t = tupleFactory.newTuple(1);
		t.set(0, bag);
		result = (String) flattenUdf.exec(t);
		assertEquals("Flatten test failed", result, "{}");

		/* check with a single element */
		t = tupleFactory.newTuple(1);
		t.set(0, "single_element");
		bag.add(t);
		t = tupleFactory.newTuple(1);
		t.set(0, bag);
		result = (String) flattenUdf.exec(t);
		assertEquals("Flatten test failed", result, "{single_element}");

		bag.clear();

		int nElements = new Random().nextInt(500) + 1;
		List<String> elements = new ArrayList<String>();
		for (int i = 0; i < nElements; i++) {
			Tuple tuple = tupleFactory.newTuple(1);
			String s = "element" + i;
			tuple.set(0, s);
			bag.add(tuple);
			elements.add(s);
		}
		result = (String) flattenUdf.exec(t);
		assertEquals("Flatten test failed", result.startsWith("{"), true);
		assertEquals("Flatten test failed", result.endsWith("}"), true);
		result = result.substring(1,result.length()-1);
		String[] splits = result.split(",");
		for (int i = 0; i < nElements; i++) {
			String split = splits[i];
			String testInput = elements.get(i);
			assertEquals("Flatten test failed", split, testInput);
		}
	}

	@After
	public void done() {
		System.out.println("*** Done with the FlattenAsGBString tests ***");
	}

}
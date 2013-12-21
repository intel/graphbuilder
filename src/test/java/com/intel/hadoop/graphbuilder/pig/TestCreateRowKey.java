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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCreateRowKey {
	EvalFunc<?> rowKeyAssignerUDF;

	@Before
	public void setup() throws Exception {
		System.out.println("*** Starting CreateRowKey tests. ***");
		rowKeyAssignerUDF = (EvalFunc<?>) PigContext
				.instantiateFuncFromSpec("com.intel.pig.udf.eval.CreateRowKey");
	}

	@Test
	public void runTests() throws IOException {
		Tuple t = TupleFactory.getInstance().newTuple(4);
		t.set(0, "1");
		t.set(1, "test_field");
		t.set(2, new Double(3.0));
		t.set(3, new Float(2.4));
		Tuple result = (Tuple) rowKeyAssignerUDF.exec(t);
		assertEquals("result must be of size 5", result.size(), 5);
		assertEquals("Tuple fields must match", result.get(1), t.get(0));
		assertEquals("Tuple fields must match", result.get(2), t.get(1));
		assertEquals("Tuple fields must match", result.get(3), t.get(2));
		assertEquals("Tuple fields must match", result.get(4), t.get(3));
		System.out.println("row key " + result.get(0));

		Tuple tupleWithNullElement = TupleFactory.getInstance().newTuple(1);
		tupleWithNullElement.set(0, null);
		result = (Tuple) rowKeyAssignerUDF.exec(tupleWithNullElement);
		assertEquals("result must be of size 2", result.size(), 2);
		assertEquals("1st field must be null", result.get(1), null);
		System.out.println("row key " + result.get(0));
	}

	@After
	public void done() {
		System.out.println("*** Done with the CreateRowKey tests ***");
	}

}
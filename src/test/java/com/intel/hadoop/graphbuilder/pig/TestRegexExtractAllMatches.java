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
import java.util.Arrays;
import java.util.Iterator;
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

public class TestRegexExtractAllMatches {
	EvalFunc<?> regexpUdf;
	TupleFactory tupleFactory = TupleFactory.getInstance();

	@Before
	public void setup() throws Exception {
		System.out.println("*** Starting RegexExtractAllMatches tests. ***");
		regexpUdf = (EvalFunc<?>) PigContext
				.instantiateFuncFromSpec(
                        "com.intel.pig.udf.eval.RegexExtractAllMatches()");
	}

	@Test
	public void runTests() throws IOException {
		Tuple inTuple = TupleFactory.getInstance().newTuple(2);
		inTuple.set(0, "foo");
		inTuple.set(1, "(foo)");
		DataBag result = (DataBag) regexpUdf.exec(inTuple);
		assertEquals("RegexExtractAllMatches test failed", result.size(), 1);
		Iterator<Tuple> iter = result.iterator();
		while (iter.hasNext()) {
			Tuple t = iter.next();
			String matched = (String) t.get(0);
			assertEquals("RegexExtractAllMatches test failed", matched, "foo");
			break;
		}

		inTuple.set(0, "<html><body><a>link_1</a></body></html>");
		inTuple.set(1, "<a>(.*?)</a>");
		result = (DataBag) regexpUdf.exec(inTuple);
		assertEquals("RegexExtractAllMatches test failed", result.size(), 1);
		iter = result.iterator();
		while (iter.hasNext()) {
			Tuple t = iter.next();
			String matched = (String) t.get(0);
			assertEquals("RegexExtractAllMatches test failed", matched,
					"link_1");
			break;
		}

		int nElements = new Random().nextInt(500) + 1;
		List<String> expectedOutput = new ArrayList<String>();
		StringBuffer testInput = new StringBuffer();
		for (int i = 0; i < nElements; i++) {
			String s = "test_" + i;
			expectedOutput.add(s);
			testInput.append("<test>").append(s).append("</test>");
		}

		inTuple.set(0, testInput.toString());
		inTuple.set(1, "<test>(.*?)</test>");
		result = (DataBag) regexpUdf.exec(inTuple);
		assertEquals("RegexExtractAllMatches test failed",
                result.size(),
                nElements);

		iter = result.iterator();
		while (iter.hasNext()) {
			Tuple t = iter.next();
			String matched = (String) t.get(0);
			assertEquals("RegexExtractAllMatches test failed",
					expectedOutput.contains(matched), true);
			expectedOutput.remove(matched);
		}
		assertEquals("RegexExtractAllMatches test failed",
				expectedOutput.size(), 0);

	}

	@Test(expected = IOException.class)
	public void testFailureCase1() throws IOException {
		System.out.println("Testing failure cases");
		/* RegexExtractAllMatches accepts only 2 parameters */
		Tuple inTuple = TupleFactory.getInstance().newTuple(3);
		regexpUdf.exec(inTuple);
	}

	public void testFailureCase2() throws IOException {
		System.out.println("Testing failure cases");
		Tuple inTuple = TupleFactory.getInstance().newTuple(2);
		inTuple.set(0, null);
		Object result = regexpUdf.exec(inTuple);
		assertEquals("RegexExtractAllMatches test failed", result, null);
	}

	@Test(expected = IOException.class)
	public void testFailureCase3() throws IOException {
		System.out.println("Testing failure cases");
		Tuple inTuple = TupleFactory.getInstance().newTuple(2);
		inTuple.set(0, "source_string");
		inTuple.set(1, "***");
		regexpUdf.exec(inTuple);
	}

	@Test(expected = IOException.class)
	public void testFailureCase4() throws IOException {
		System.out.println("Testing failure cases");
		Tuple inTuple = TupleFactory.getInstance().newTuple(2);
		inTuple.set(0, "source_string");
		inTuple.set(1, null);
		regexpUdf.exec(inTuple);
	}

	@After
	public void done() {
		System.out
				.println("*** Done with the RegexExtractAllMatches tests ***");
	}

}
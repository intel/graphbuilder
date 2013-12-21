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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.intel.hadoop.graphbuilder.graphelements.VertexID;
import org.apache.pig.EvalFunc;
import org.apache.pig.impl.PigContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.pig.data.PropertyGraphElementTuple;

public class TestVertexlistUdf {
    EvalFunc<?> toEdgelistUdf0;
    EvalFunc<?> toEdgelistUdf1;

    @Before
    public void setup() throws Exception {
        System.out.println("*** Starting VertexList test cases ***");
        toEdgelistUdf0 = (EvalFunc<?>) PigContext
                .instantiateFuncFromSpec(
                        "com.intel.pig.udf.eval.VertexList('false')");
        toEdgelistUdf1 = (EvalFunc<?>) PigContext
                .instantiateFuncFromSpec(
                        "com.intel.pig.udf.eval.VertexList('true')");
    }

    @Test
    public void runTests() throws IOException {
        SerializedGraphElementStringTypeVids serializedGraphElement
                = new SerializedGraphElementStringTypeVids();

        PropertyMap map0 = new PropertyMap();
        map0.setProperty("name", new StringType("Alice"));
        map0.setProperty("age", new StringType("30"));

        Vertex<StringType> vertex = new Vertex<StringType>();
        vertex.configure( new VertexID<StringType>(new StringType("Employee001")),
                map0);
        vertex.setLabel(new StringType("HAWK.People"));

        serializedGraphElement.init(vertex);

        PropertyGraphElementTuple t = new PropertyGraphElementTuple(1);
        t.set(0, serializedGraphElement);

        String statement0 = (String) toEdgelistUdf0.exec(t);
        assertEquals(
                    "Vertex tuple mismatch",
                    statement0,
                    "Employee001\tHAWK.People");

        String statement1 = (String) toEdgelistUdf1.exec(t);
        System.out.println(statement1);
        boolean flag = statement1.contains("HAWK.People.Employee001");
        assertTrue("Vertex tuple mismatch", flag);
        flag = statement1.contains("name:Alice");
        assertTrue("Vertex tuple mismatch", flag);
        flag = statement1.contains("age:30");
        assertTrue("Vertex tuple mismatch", flag);
    }

    @After
    public void done() {
        System.out.println("*** Done with the VertexList tests ***");
    }

}
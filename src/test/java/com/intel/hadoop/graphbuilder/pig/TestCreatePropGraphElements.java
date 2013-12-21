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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.pig.data.PropertyGraphElementTuple;
import com.intel.hadoop.graphbuilder.types.*;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCreatePropGraphElements
{
    EvalFunc<?> createPropGraphElementsUDF;

    @Before
    public void setup() throws Exception {
        System.out.println("*** Starting CreatePropGraphElements tests. ***");
        createPropGraphElementsUDF = (EvalFunc<?>) PigContext
                .instantiateFuncFromSpec(
                new FuncSpec("com.intel.pig.udf.eval.CreatePropGraphElements",
                "-v name=age,managerId -e name,department,worksAt,tenure"));
    }

    @Test
    public void runTests() throws IOException {

        Schema.FieldSchema idField
                = new Schema.FieldSchema("id", DataType.INTEGER);
        Schema.FieldSchema nameField
                = new Schema.FieldSchema("name", DataType.CHARARRAY);
        Schema.FieldSchema ageField
                = new Schema.FieldSchema("age", DataType.INTEGER);
        Schema.FieldSchema managerIdField
                = new Schema.FieldSchema("managerId", DataType.CHARARRAY);
        Schema.FieldSchema tenureField
                = new Schema.FieldSchema("tenure", DataType.CHARARRAY);
        Schema.FieldSchema departmentField
                = new Schema.FieldSchema("department", DataType.CHARARRAY);

        List fsList = asList(idField,
                             nameField,
                             ageField,
                             managerIdField,
                             tenureField,
                             departmentField);

        Schema schema = new Schema(fsList);

        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple t = TupleFactory.getInstance().newTuple(6);

        Integer id = 1;
        String name = "Haywood Y. Buzzov";
        int age = 33;
        String managerId = "Ivanna B. Onatop";
        String tenure =  "Four score and seven years";
        String department = "Overoptimized Commodities";

        t.set(0, id);
        t.set(1, name);
        t.set(2, age);
        t.set(3, managerId);
        t.set(4, tenure);
        t.set(5, department);

        DataBag result = (DataBag) createPropGraphElementsUDF.exec(t);

        assert(result.size() == 5);
    }

    @After
    public void done() {
        System.out.println("*** Done with the CreatePropGraphElements tests ***");
    }

}
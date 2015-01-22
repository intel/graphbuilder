/* Copyright (C) 2013 Intel Corporation.
 * Copyright 2014 YarcData LLC
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.pig.udf.eval.CreatePropGraphElements;
import com.intel.pig.udf.eval.mappings.EdgeMapping;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link CreatePropGraphElements} UDF
 * 
 */
public class TestCreatePropGraphElements {
    EvalFunc<?> createPropGraphElementsUDF;

    /**
     * Create an instance of the UDF for use in the test
     * 
     * @throws Exception
     */
    @Before
    public void setup() throws Exception {
        createPropGraphElementsUDF = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(new FuncSpec(
                "com.intel.pig.udf.eval.CreatePropGraphElements"));
    }

    protected Schema prepareSchema() {
        Schema.FieldSchema idField = new Schema.FieldSchema("id", DataType.INTEGER);
        Schema.FieldSchema nameField = new Schema.FieldSchema("name", DataType.CHARARRAY);
        Schema.FieldSchema ageField = new Schema.FieldSchema("age", DataType.INTEGER);
        Schema.FieldSchema managerIdField = new Schema.FieldSchema("managerId", DataType.CHARARRAY);
        Schema.FieldSchema tenureField = new Schema.FieldSchema("tenure", DataType.CHARARRAY);
        Schema.FieldSchema departmentField = new Schema.FieldSchema("department", DataType.CHARARRAY);
        Schema.FieldSchema mappingField = new Schema.FieldSchema(null, DataType.MAP);

        List<FieldSchema> fsList = asList(idField, nameField, ageField, managerIdField, tenureField, departmentField,
                mappingField);
        return new Schema(fsList);
    }

    protected List<String> prepareVertexMappingProperties() {
        return this.prepareVertexMappingProperties(new String[] { "age", "managerId", "department", "tenure" });
    }

    protected List<String> prepareVertexMappingProperties(String[] properties) {
        List<String> ps = new ArrayList<>();
        for (String p : properties) {
            ps.add(p);
        }
        return ps;
    }

    protected Map<String, Object> prepareVertexMapping() {
        return this.prepareVertexMapping(this.prepareVertexMappingProperties());
    }

    protected Map<String, Object> prepareVertexMapping(List<String> properties) {
        Map<String, Object> vertexMapping = new HashMap<String, Object>();
        vertexMapping.put("id", "name");
        vertexMapping.put("properties", TupleFactory.getInstance().newTuple(properties));
        return vertexMapping;
    }

    protected Map<String, Object> prepareMapping() {
        List<Map<String, Object>> vertexMappings = new ArrayList<Map<String, Object>>();
        vertexMappings.add(this.prepareVertexMapping());
        return this.prepareMappings(vertexMappings, new ArrayList<Map<String, Object>>());
    }

    protected Map<String, Object> prepareMappings(List<Map<String, Object>> vertexMappings, List<Map<String, Object>> edgeMappings) {
        Map<String, Object> mapping = new HashMap<String, Object>();

        mapping.put("vertices", TupleFactory.getInstance().newTuple(vertexMappings));
        mapping.put("edges", TupleFactory.getInstance().newTuple(edgeMappings));
        return mapping;
    }

    protected Tuple prepareData() throws ExecException {
        return this.prepareData(this.prepareMapping());
    }

    protected Tuple prepareData(Map<String, Object> mapping) throws ExecException {
        Tuple t = TupleFactory.getInstance().newTuple(7);

        Integer id = 1;
        String name = "Haywood Y. Buzzov";
        int age = 33;
        String managerId = "Ivanna B. Onatop";
        String tenure = "Four score and seven years";
        String department = "Overoptimized Commodities";

        t.set(0, id);
        t.set(1, name);
        t.set(2, age);
        t.set(3, managerId);
        t.set(4, tenure);
        t.set(5, department);
        t.set(6, mapping);
        
        return t;
    }

    protected List<Tuple> checkResults(Tuple t, int expectedTuples, int[] expectedProperties) throws IOException {
        DataBag result = (DataBag) createPropGraphElementsUDF.exec(t);
        List<Tuple> ts = new ArrayList<Tuple>();
        
        Assert.assertEquals(expectedTuples, result.size());

        if (expectedProperties.length < expectedTuples)
            Assert.fail("Insufficient expected property counts provided");

        Iterator<Tuple> iter = result.iterator();
        int i = 0;
        while (iter.hasNext()) {
            Tuple tuple = iter.next();
            ts.add(tuple);
            int expected = expectedProperties[i];

            // Check size of tuple and the contained graph element
            Assert.assertEquals(1, tuple.size());
            SerializedGraphElement<?> element = (SerializedGraphElement<?>) tuple.get(0);
            //System.out.println(element.toString());
            Assert.assertNotNull(element.graphElement());
            Assert.assertEquals(expected, element.graphElement().getProperties().size());

            i++;
        }
        
        return ts;
    }

    @Test
    public void direct_tuple_01() throws IOException {
        Schema schema = this.prepareSchema();
        createPropGraphElementsUDF.setInputSchema(schema);
        checkResults(this.prepareData(), 1, new int[] { 4 });
    }

    @Test
    public void direct_tuple_02() throws IOException {
        Schema schema = this.prepareSchema();
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple tuple = this.prepareData();
        tuple.set(1, null);
        checkResults(tuple, 0, new int[0]);
    }

    @Test
    public void direct_tuple_03() throws IOException {
        Schema schema = this.prepareSchema();
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple tuple = this.prepareData();
        tuple.set(2, null);
        checkResults(tuple, 1, new int[] { 3 });
    }

    @Test
    public void direct_tuple_04() throws IOException {
        Schema schema = this.prepareSchema();
        createPropGraphElementsUDF.setInputSchema(schema);

        Map<String, Object> vertexMapping = this
                .prepareVertexMapping(this.prepareVertexMappingProperties(new String[] { "age" }));
        List<Map<String, Object>> vertexMappings = new ArrayList<Map<String, Object>>();
        vertexMappings.add(vertexMapping);

        checkResults(this.prepareData(this.prepareMappings(vertexMappings, new ArrayList<Map<String, Object>>())), 1,
                new int[] { 1 });
    }
    
    @Test
    public void direct_tuple_05() throws IOException {
        Schema schema = this.prepareSchema();
        createPropGraphElementsUDF.setInputSchema(schema);

        Map<String, Object> edgeMapping = new EdgeMapping("id", "managerId", "hasManager", null, null, null, false).toMap();
        List<Map<String, Object>> edgeMappings = new ArrayList<Map<String, Object>>();
        edgeMappings.add(edgeMapping);

        checkResults(this.prepareData(this.prepareMappings(new ArrayList<Map<String, Object>>(), edgeMappings)), 3,
                new int[] { 0, 0, 0 });
    }
    
    @Test
    public void direct_tuple_06() throws IOException {
        Schema schema = this.prepareSchema();
        createPropGraphElementsUDF.setInputSchema(schema);

        Map<String, Object> edgeMapping = new EdgeMapping("id", "managerId", "hasManager", "manages", null, null, false).toMap();
        List<Map<String, Object>> edgeMappings = new ArrayList<Map<String, Object>>();
        edgeMappings.add(edgeMapping);

        checkResults(this.prepareData(this.prepareMappings(new ArrayList<Map<String, Object>>(), edgeMappings)), 4,
                new int[] { 0, 0, 0, 0 });
    }

    @Test
    public void indirect_tuple_01() throws IOException {
        Schema innerSchema = this.prepareSchema();
        Schema schema = new Schema(new FieldSchema(null, innerSchema, DataType.TUPLE));
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple innerTuple = this.prepareData();
        Tuple tuple = TupleFactory.getInstance().newTuple(innerTuple);

        this.checkResults(tuple, 1, new int[] { 4 });
    }

    @Test
    public void indirect_tuple_02() throws IOException {
        Schema innerSchema = this.prepareSchema();
        Schema schema = new Schema(new FieldSchema(null, innerSchema, DataType.TUPLE));
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple innerTuple = this.prepareData();
        innerTuple.set(1, null);
        Tuple tuple = TupleFactory.getInstance().newTuple(innerTuple);

        this.checkResults(tuple, 0, new int[0]);
    }

    @Test
    public void indirect_tuple_03() throws IOException {
        Schema innerSchema = this.prepareSchema();
        Schema schema = new Schema(new FieldSchema(null, innerSchema, DataType.TUPLE));
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple innerTuple = this.prepareData();
        innerTuple.set(2, null);
        Tuple tuple = TupleFactory.getInstance().newTuple(innerTuple);

        this.checkResults(tuple, 1, new int[] { 3 });
    }
}
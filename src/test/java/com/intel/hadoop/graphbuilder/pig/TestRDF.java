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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
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

import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.intel.pig.udf.eval.RDF;
import com.intel.pig.udf.eval.mappings.RdfMapping;
import com.intel.pig.udf.eval.mappings.RdfMapping.EdgePropertiesMode;

/**
 * Tests for the {@link RDF} UDF
 * 
 */
public class TestRDF extends TestCreatePropGraphElements {
    EvalFunc<?> toRdfUdf;

    @Before
    public void setup() throws Exception {
        super.setup();
        toRdfUdf = (EvalFunc<?>) PigContext.instantiateFuncFromSpec("com.intel.pig.udf.eval.RDF");
    }

    private Schema prepareRdfSchema(Schema baseSchema) {
        List<FieldSchema> fields = new ArrayList<>();
        fields.addAll(baseSchema.getFields());
        FieldSchema mappingField = new Schema.FieldSchema(null, DataType.MAP);
        fields.add(mappingField);

        return new Schema(fields);
    }

    private List<Tuple> checkRdfResults(Tuple t, int expectedTriples, String[] expectedTripleData) throws IOException {
        DataBag result = (DataBag) toRdfUdf.exec(t);
        List<Tuple> ts = new ArrayList<Tuple>();

        Assert.assertNotNull(result);
        Assert.assertEquals(expectedTriples, result.size());

        if (expectedTripleData.length < expectedTriples)
            Assert.fail("Insufficient expected triple data provided");

        Iterator<Tuple> iter = result.iterator();
        int i = 0;
        while (iter.hasNext()) {
            Tuple tuple = iter.next();
            ts.add(tuple);
            String expected = expectedTripleData[i];

            // Check size of tuple
            Assert.assertEquals(1, tuple.size());
            String actual = (String) tuple.get(0);
            Assert.assertEquals(expected, actual);

            i++;
        }

        return ts;
    }

    @Test
    public void indirect_rdf_tuple_01() throws IOException {
        // First create a property graph
        Schema innerSchema = this.prepareSchema();
        Schema schema = new Schema(new FieldSchema(null, innerSchema, DataType.TUPLE));
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple innerTuple = this.prepareData();
        Tuple tuple = TupleFactory.getInstance().newTuple(innerTuple);

        List<Tuple> propertyGraphResults = this.checkResults(tuple, 1, new int[] { 4 });
        Assert.assertEquals(1, propertyGraphResults.size());

        // Then convert to a RDF graph
        // Need to prepare the appropriate input schema
        Schema propertyGraphSchema = createPropGraphElementsUDF.outputSchema(schema);
        Schema rdfInputSchema = this.prepareRdfSchema(propertyGraphSchema);
        toRdfUdf.setInputSchema(rdfInputSchema);

        // And need to add a RDF mapping to the data
        Tuple rdfInputTuple = TupleFactory.getInstance().newTuple(2);
        rdfInputTuple.set(0, propertyGraphResults.get(0).get(0));
        RdfMapping mapping = new RdfMapping("http://example.org/ontology#", "http://example.org/instances#",
                new HashMap<String, String>(), true, Arrays.asList("age"), new ArrayList<String>(),
                new HashMap<String, String>(), null, null, EdgePropertiesMode.IGNORE);
        rdfInputTuple.set(1, mapping.toMap());
        rdfInputTuple = TupleFactory.getInstance().newTuple(rdfInputTuple);

        // Convert to RDF and check
        this.checkRdfResults(
                rdfInputTuple,
                1,
                new String[] { "<http://example.org/instances#Haywood%20Y.%20Buzzov> <http://example.org/ontology#age> 33 ." });
    }

    @Test
    public void indirect_rdf_tuple_02() throws IOException {
        // First create a property graph
        Schema innerSchema = this.prepareSchema();
        Schema schema = new Schema(new FieldSchema(null, innerSchema, DataType.TUPLE));
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple innerTuple = this.prepareData();
        Tuple tuple = TupleFactory.getInstance().newTuple(innerTuple);

        List<Tuple> propertyGraphResults = this.checkResults(tuple, 1, new int[] { 4 });
        Assert.assertEquals(1, propertyGraphResults.size());

        // Then convert to a RDF graph
        // Need to prepare the appropriate input schema
        Schema propertyGraphSchema = createPropGraphElementsUDF.outputSchema(schema);
        Schema rdfInputSchema = this.prepareRdfSchema(propertyGraphSchema);
        toRdfUdf.setInputSchema(rdfInputSchema);

        // And need to add a RDF mapping to the data
        Tuple rdfInputTuple = TupleFactory.getInstance().newTuple(2);
        rdfInputTuple.set(0, propertyGraphResults.get(0).get(0));
        Map<String, String> propertyMappings = new HashMap<String, String>();
        propertyMappings.put("age", "foaf:age");
        Map<String, String> namespaces = new HashMap<String, String>();
        namespaces.put("foaf", FOAF.NS);
        RdfMapping mapping = new RdfMapping("http://example.org/ontology#", "http://example.org/instances#",
                namespaces, true, Arrays.asList("age"), new ArrayList<String>(), propertyMappings, null, null,
                EdgePropertiesMode.IGNORE);
        rdfInputTuple.set(1, mapping.toMap());
        rdfInputTuple = TupleFactory.getInstance().newTuple(rdfInputTuple);

        // Convert to RDF and check
        this.checkRdfResults(
                rdfInputTuple,
                1,
                new String[] { "<http://example.org/instances#Haywood%20Y.%20Buzzov> <http://xmlns.com/foaf/0.1/age> 33 ." });
    }

    @Test
    public void indirect_rdf_tuple_03() throws IOException {
        // First create a property graph
        Schema innerSchema = this.prepareSchema();
        Schema schema = new Schema(new FieldSchema(null, innerSchema, DataType.TUPLE));
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple innerTuple = this.prepareData();
        Tuple tuple = TupleFactory.getInstance().newTuple(innerTuple);

        List<Tuple> propertyGraphResults = this.checkResults(tuple, 1, new int[] { 4 });
        Assert.assertEquals(1, propertyGraphResults.size());

        // Then convert to a RDF graph
        // Need to prepare the appropriate input schema
        Schema propertyGraphSchema = createPropGraphElementsUDF.outputSchema(schema);
        Schema rdfInputSchema = this.prepareRdfSchema(propertyGraphSchema);
        toRdfUdf.setInputSchema(rdfInputSchema);

        // And need to add a RDF mapping to the data
        Tuple rdfInputTuple = TupleFactory.getInstance().newTuple(2);
        rdfInputTuple.set(0, propertyGraphResults.get(0).get(0));
        Map<String, String> propertyMappings = new HashMap<String, String>();
        propertyMappings.put("age", "foaf:age");
        propertyMappings.put("name", "foaf:name");
        Map<String, String> namespaces = new HashMap<String, String>();
        namespaces.put("foaf", FOAF.NS);
        RdfMapping mapping = new RdfMapping("http://example.org/ontology#", "http://example.org/instances#",
                namespaces, true, Arrays.asList("age"), new ArrayList<String>(), propertyMappings, null, "name",
                EdgePropertiesMode.IGNORE);
        rdfInputTuple.set(1, mapping.toMap());
        rdfInputTuple = TupleFactory.getInstance().newTuple(rdfInputTuple);

        // Convert to RDF and check
        this.checkRdfResults(
                rdfInputTuple,
                1,
                new String[] { "<http://example.org/instances#Haywood%20Y.%20Buzzov> <http://xmlns.com/foaf/0.1/age> 33 ." });
    }

    @Test
    public void indirect_rdf_tuple_04() throws IOException {
        // First create a property graph
        Schema innerSchema = this.prepareSchema();
        Schema schema = new Schema(new FieldSchema(null, innerSchema, DataType.TUPLE));
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple innerTuple = this.prepareData();
        Tuple tuple = TupleFactory.getInstance().newTuple(innerTuple);

        List<Tuple> propertyGraphResults = this.checkResults(tuple, 1, new int[] { 4 });
        Assert.assertEquals(1, propertyGraphResults.size());

        // Then convert to a RDF graph
        // Need to prepare the appropriate input schema
        Schema propertyGraphSchema = createPropGraphElementsUDF.outputSchema(schema);
        Schema rdfInputSchema = this.prepareRdfSchema(propertyGraphSchema);
        toRdfUdf.setInputSchema(rdfInputSchema);

        // And need to add a RDF mapping to the data
        Tuple rdfInputTuple = TupleFactory.getInstance().newTuple(2);
        rdfInputTuple.set(0, propertyGraphResults.get(0).get(0));
        RdfMapping mapping = new RdfMapping("http://example.org/ontology#", "http://example.org/instances#", null,
                true, Arrays.asList("age"), null, null, Arrays.asList("age"), "name", EdgePropertiesMode.IGNORE);
        rdfInputTuple.set(1, mapping.toMap());
        rdfInputTuple = TupleFactory.getInstance().newTuple(rdfInputTuple);

        // Convert to RDF and check
        this.checkRdfResults(
                rdfInputTuple,
                1,
                new String[] { "<http://example.org/instances#Haywood%20Y.%20Buzzov> <http://example.org/ontology#age> <http://example.org/ontology#33> ." });
    }

    @Test
    public void indirect_rdf_tuple_05() throws IOException {
        // First create a property graph
        Schema innerSchema = this.prepareSchema();
        Schema schema = new Schema(new FieldSchema(null, innerSchema, DataType.TUPLE));
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple innerTuple = this.prepareData();
        Tuple tuple = TupleFactory.getInstance().newTuple(innerTuple);

        List<Tuple> propertyGraphResults = this.checkResults(tuple, 1, new int[] { 4 });
        Assert.assertEquals(1, propertyGraphResults.size());

        // Then convert to a RDF graph
        // Need to prepare the appropriate input schema
        Schema propertyGraphSchema = createPropGraphElementsUDF.outputSchema(schema);
        Schema rdfInputSchema = this.prepareRdfSchema(propertyGraphSchema);
        toRdfUdf.setInputSchema(rdfInputSchema);

        // And need to add a RDF mapping to the data
        Tuple rdfInputTuple = TupleFactory.getInstance().newTuple(2);
        rdfInputTuple.set(0, propertyGraphResults.get(0).get(0));
        Map<String, String> propertyMappings = new HashMap<String, String>();
        propertyMappings.put("age", "foo/bar/age");
        propertyMappings.put("name", "foo/bar/name");
        RdfMapping mapping = new RdfMapping("http://example.org/ontology#", "http://example.org/instances#", null,
                true, Arrays.asList("age"), new ArrayList<String>(), propertyMappings, null, "name",
                EdgePropertiesMode.IGNORE);
        rdfInputTuple.set(1, mapping.toMap());
        rdfInputTuple = TupleFactory.getInstance().newTuple(rdfInputTuple);

        // Convert to RDF and check
        this.checkRdfResults(
                rdfInputTuple,
                1,
                new String[] { "<http://example.org/instances#Haywood%20Y.%20Buzzov> <http://example.org/foo/bar/age> 33 ." });
    }

    @Test
    public void indirect_rdf_tuple_06() throws IOException {
        // First create a property graph
        Schema innerSchema = this.prepareSchema();
        Schema schema = new Schema(new FieldSchema(null, innerSchema, DataType.TUPLE));
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple innerTuple = this.prepareData();
        Tuple tuple = TupleFactory.getInstance().newTuple(innerTuple);

        List<Tuple> propertyGraphResults = this.checkResults(tuple, 1, new int[] { 4 });
        Assert.assertEquals(1, propertyGraphResults.size());

        // Then convert to a RDF graph
        // Need to prepare the appropriate input schema
        Schema propertyGraphSchema = createPropGraphElementsUDF.outputSchema(schema);
        Schema rdfInputSchema = this.prepareRdfSchema(propertyGraphSchema);
        toRdfUdf.setInputSchema(rdfInputSchema);

        // And need to add a RDF mapping to the data
        Tuple rdfInputTuple = TupleFactory.getInstance().newTuple(2);
        rdfInputTuple.set(0, propertyGraphResults.get(0).get(0));
        Map<String, String> propertyMappings = new HashMap<String, String>();
        propertyMappings.put("age", "#age");
        propertyMappings.put("name", "#name");
        RdfMapping mapping = new RdfMapping("http://example.org/ontology#", "http://example.org/instances#", null,
                true, Arrays.asList("age"), new ArrayList<String>(), propertyMappings, null, "name",
                EdgePropertiesMode.IGNORE);
        rdfInputTuple.set(1, mapping.toMap());
        rdfInputTuple = TupleFactory.getInstance().newTuple(rdfInputTuple);

        // Convert to RDF and check
        this.checkRdfResults(
                rdfInputTuple,
                1,
                new String[] { "<http://example.org/instances#Haywood%20Y.%20Buzzov> <http://example.org/ontology#age> 33 ." });
    }

    @Test
    public void indirect_rdf_tuple_07() throws IOException {
        // First create a property graph
        Schema innerSchema = this.prepareSchema();
        Schema schema = new Schema(new FieldSchema(null, innerSchema, DataType.TUPLE));
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple innerTuple = this.prepareData();
        Tuple tuple = TupleFactory.getInstance().newTuple(innerTuple);

        List<Tuple> propertyGraphResults = this.checkResults(tuple, 1, new int[] { 4 });
        Assert.assertEquals(1, propertyGraphResults.size());

        // Then convert to a RDF graph
        // Need to prepare the appropriate input schema
        Schema propertyGraphSchema = createPropGraphElementsUDF.outputSchema(schema);
        Schema rdfInputSchema = this.prepareRdfSchema(propertyGraphSchema);
        toRdfUdf.setInputSchema(rdfInputSchema);

        // And need to add a RDF mapping to the data
        Tuple rdfInputTuple = TupleFactory.getInstance().newTuple(2);
        rdfInputTuple.set(0, propertyGraphResults.get(0).get(0));
        Map<String, String> propertyMappings = new HashMap<String, String>();
        propertyMappings.put("age", "#foo/bar/age");
        propertyMappings.put("name", "#foo/bar/name");
        RdfMapping mapping = new RdfMapping("http://example.org/ontology#", "http://example.org/instances#", null,
                true, Arrays.asList("age"), new ArrayList<String>(), propertyMappings, null, "name",
                EdgePropertiesMode.IGNORE);
        rdfInputTuple.set(1, mapping.toMap());
        rdfInputTuple = TupleFactory.getInstance().newTuple(rdfInputTuple);

        // Convert to RDF and check
        this.checkRdfResults(
                rdfInputTuple,
                1,
                new String[] { "<http://example.org/instances#Haywood%20Y.%20Buzzov> <http://example.org/ontology#foo/bar/age> 33 ." });
    }

    @Test
    public void indirect_rdf_tuple_08() throws IOException {
        // First create a property graph
        Schema innerSchema = this.prepareSchema();
        Schema schema = new Schema(new FieldSchema(null, innerSchema, DataType.TUPLE));
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple innerTuple = this.prepareData();
        Tuple tuple = TupleFactory.getInstance().newTuple(innerTuple);

        List<Tuple> propertyGraphResults = this.checkResults(tuple, 1, new int[] { 4 });
        Assert.assertEquals(1, propertyGraphResults.size());

        // Then convert to a RDF graph
        // Need to prepare the appropriate input schema
        Schema propertyGraphSchema = createPropGraphElementsUDF.outputSchema(schema);
        Schema rdfInputSchema = this.prepareRdfSchema(propertyGraphSchema);
        toRdfUdf.setInputSchema(rdfInputSchema);

        // And need to add a RDF mapping to the data
        Tuple rdfInputTuple = TupleFactory.getInstance().newTuple(2);
        rdfInputTuple.set(0, propertyGraphResults.get(0).get(0));
        Map<String, String> propertyMappings = new HashMap<String, String>();
        propertyMappings.put("age", "?age");
        propertyMappings.put("name", "?name");
        RdfMapping mapping = new RdfMapping("http://example.org/ontology#", "http://example.org/instances#", null,
                true, Arrays.asList("age"), new ArrayList<String>(), propertyMappings, null, "name",
                EdgePropertiesMode.IGNORE);
        rdfInputTuple.set(1, mapping.toMap());
        rdfInputTuple = TupleFactory.getInstance().newTuple(rdfInputTuple);

        // Convert to RDF and check
        this.checkRdfResults(
                rdfInputTuple,
                1,
                new String[] { "<http://example.org/instances#Haywood%20Y.%20Buzzov> <http://example.org/ontology?age> 33 ." });
    }

    @Test
    public void indirect_rdf_tuple_09() throws IOException {
        // First create a property graph
        Schema innerSchema = this.prepareSchema();
        Schema schema = new Schema(new FieldSchema(null, innerSchema, DataType.TUPLE));
        createPropGraphElementsUDF.setInputSchema(schema);

        Tuple innerTuple = this.prepareData();
        Tuple tuple = TupleFactory.getInstance().newTuple(innerTuple);

        List<Tuple> propertyGraphResults = this.checkResults(tuple, 1, new int[] { 4 });
        Assert.assertEquals(1, propertyGraphResults.size());

        // Then convert to a RDF graph
        // Need to prepare the appropriate input schema
        Schema propertyGraphSchema = createPropGraphElementsUDF.outputSchema(schema);
        Schema rdfInputSchema = this.prepareRdfSchema(propertyGraphSchema);
        toRdfUdf.setInputSchema(rdfInputSchema);

        // And need to add a RDF mapping to the data
        Tuple rdfInputTuple = TupleFactory.getInstance().newTuple(2);
        rdfInputTuple.set(0, propertyGraphResults.get(0).get(0));
        Map<String, String> propertyMappings = new HashMap<String, String>();
        propertyMappings.put("age", "age");
        propertyMappings.put("name", "name");
        RdfMapping mapping = new RdfMapping("http://example.org/ontology?", "http://example.org/instances#", null,
                true, Arrays.asList("age"), new ArrayList<String>(), propertyMappings, null, "name", EdgePropertiesMode.IGNORE);
        rdfInputTuple.set(1, mapping.toMap());
        rdfInputTuple = TupleFactory.getInstance().newTuple(rdfInputTuple);

        // Convert to RDF and check
        this.checkRdfResults(rdfInputTuple, 1,
                new String[] { "<http://example.org/instances#Haywood%20Y.%20Buzzov> <http://example.org/age> 33 ." });
    }
}
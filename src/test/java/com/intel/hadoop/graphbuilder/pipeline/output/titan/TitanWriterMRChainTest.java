/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pipeline.output.titan;


import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementLongTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.pipeline.TestMapReduceDriverUtils;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.GBHTableConfiguration;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.thinkaurelius.titan.core.TitanElement;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TitanElement.class)
public class TitanWriterMRChainTest extends TestMapReduceDriverUtils {
    SerializedGraphElementLongTypeVids graphElement;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        graphElement = new SerializedGraphElementLongTypeVids();
    }

    @After
    public void tearDown(){
        super.tearDown();
        graphElement = null;
    }

    @Test
    public void test_hbase_to_vertices_to_titan_MR() throws Exception {

        Pair<ImmutableBytesWritable, Result> alice =
                new Pair<ImmutableBytesWritable, Result>(new ImmutableBytesWritable(Bytes.toBytes("row1")), sampleDataAlice());

        Pair<ImmutableBytesWritable, Result>[] pairs = new Pair[]{alice};



        com.tinkerpop.blueprints.Vertex  bpVertex = vertexMock();

        PowerMockito.doReturn(bpVertex).when(titanGraph).addVertex(null);

        PowerMockito.doReturn(900L).doReturn(901L).doReturn(902L).when(spiedTitanMergedGraphElementWrite).getVertexId
                (any(com.tinkerpop.blueprints.Vertex.class));

        List<Pair<IntWritable,SerializedGraphElement>> run =
                runVertexHbaseMR(pairs);

        assertTrue("check the number of writables", run.size() == 3);
        assertEquals("check the number of counters", gbVertexMapReduceDriver.getCounters().countCounters(), 4);
        assertEquals("check HTABLE_COLS_READ counter", gbVertexMapReduceDriver.getCounters().findCounter
                (GBHTableConfiguration.Counters.HTABLE_COLS_READ).getValue(), 5);
        assertEquals("check HTABLE_ROWS_READ counter", gbVertexMapReduceDriver.getCounters().findCounter
                (GBHTableConfiguration.Counters.HTABLE_ROWS_READ).getValue(), 1);
        assertEquals("check NUM_EDGES counter", gbVertexMapReduceDriver.getCounters().findCounter
                (spiedVerticesIntoTitanReducer.getEdgeCounter()).getValue(), 1);
        assertEquals("check NUM_VERTICES counter", gbVertexMapReduceDriver.getCounters().findCounter
                (spiedVerticesIntoTitanReducer.getVertexCounter()).getValue(), 2);

        //set up our matching vertex to test against
        com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType> vertex =
                new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType("Alice"));
        vertex.setProperty("cf:dept", new StringType("GAO123"));
        vertex.setProperty("cf:age", new StringType("43"));
        vertex.setProperty("TitanID", new LongType(900L));

        graphElement.init(vertex);

        verifyPairSecond(run.get(0), graphElement);

        //set up our matching edge to test against
        com.intel.hadoop.graphbuilder.graphelements.Edge<StringType> edge = new com.intel.hadoop.graphbuilder.graphelements.Edge<StringType>(new StringType("Alice"),
                new StringType("GAO123"),
                new StringType("worksAt"));
        edge.setProperty("srcTitanID", new LongType(900L));

        graphElement.init(edge);

        verifyPairSecond(run.get(1), graphElement);


        vertex = new com.intel.hadoop.graphbuilder.graphelements.Vertex<>(new StringType("GAO123"));
        vertex.setProperty("TitanID", new LongType(901L));

        graphElement.init(vertex);

        verifyPairSecond(run.get(2), graphElement);
    }

    @Test
    public void test_hbase_vertices_to_titan_MR_two_hbase_results() throws Exception {

        printSampleData(sampleDataAlice());
        printSampleData(sampleDataBob());
        Pair<ImmutableBytesWritable, Result> alice = new Pair<ImmutableBytesWritable, Result>
                (new ImmutableBytesWritable(Bytes.toBytes("row1")), sampleDataAlice());
        Pair<ImmutableBytesWritable, Result> bob = new Pair<ImmutableBytesWritable, Result>
                (new ImmutableBytesWritable(Bytes.toBytes("row2")), sampleDataBob());
        Pair<ImmutableBytesWritable, Result>[] pairs = new Pair[]{alice, bob};


        com.tinkerpop.blueprints.Vertex  bpVertex = vertexMock();

        PowerMockito.doReturn(bpVertex).when(titanGraph).addVertex(null);

        PowerMockito.doReturn(900L).doReturn(901L).doReturn(902L).doReturn(903L).doReturn(904L)
                .when(spiedTitanMergedGraphElementWrite).getVertexId(any(com.tinkerpop.blueprints.Vertex.class));

        List<Pair<IntWritable,SerializedGraphElement>> run = runVertexHbaseMR(pairs);

        assertTrue("check the number of writables", run.size() == 6);
        assertEquals("check the number of counters", gbVertexMapReduceDriver.getCounters().countCounters(), 4);
        assertEquals("check HTABLE_COLS_READ counter", gbVertexMapReduceDriver.getCounters().findCounter
                (GBHTableConfiguration.Counters.HTABLE_COLS_READ).getValue(), 10);
        assertEquals("check HTABLE_ROWS_READ counter", gbVertexMapReduceDriver.getCounters().findCounter
                (GBHTableConfiguration.Counters.HTABLE_ROWS_READ).getValue(), 2);
        assertEquals("check NUM_EDGES counter", gbVertexMapReduceDriver.getCounters().findCounter
                (spiedVerticesIntoTitanReducer.getEdgeCounter()).getValue(), 2);
        assertEquals("check NUM_VERTICES counter", gbVertexMapReduceDriver.getCounters().findCounter
                (spiedVerticesIntoTitanReducer.getVertexCounter()).getValue(), 4);

        //set up our matching vertex to test against
        com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType> vertex =
                new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType("INTELLABS"));
        vertex.setProperty("TitanID", new LongType(900L));

        graphElement.init(vertex);

        verifyPairSecond(run.get(0), graphElement);

        vertex = new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType("Bob"));
        vertex.setProperty("cf:dept", new StringType("INTELLABS"));
        vertex.setProperty("cf:age", new StringType("45"));
        vertex.setProperty("TitanID", new LongType(901L));

        graphElement.init(vertex);

        verifyPairSecond(run.get(1), graphElement);

        //set up our matching edge to test against
        com.intel.hadoop.graphbuilder.graphelements.Edge<StringType> edge =
                new com.intel.hadoop.graphbuilder.graphelements.Edge<StringType>(new StringType("Bob"),
                new StringType("INTELLABS"),
                new StringType("worksAt"));
        edge.setProperty("srcTitanID", new LongType(901L));

        graphElement.init(edge);

        verifyPairSecond(run.get(2), graphElement);

        vertex = new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType("Alice"));
        vertex.setProperty("cf:dept", new StringType("GAO123"));
        vertex.setProperty("cf:age", new StringType("43"));
        vertex.setProperty("TitanID", new LongType(902L));

        graphElement.init(vertex);

        verifyPairSecond(run.get(3), graphElement);

        edge = new com.intel.hadoop.graphbuilder.graphelements.Edge<StringType>(new StringType("Alice"),
                new StringType("GAO123"),
                new StringType("worksAt"));
        edge.setProperty("srcTitanID", new LongType(902L));

        graphElement.init(edge);

        verifyPairSecond(run.get(4), graphElement);


        vertex = new com.intel.hadoop.graphbuilder.graphelements.Vertex<>(new StringType("GAO123"));
        vertex.setProperty("TitanID", new LongType(903L));

        graphElement.init(vertex);

        verifyPairSecond(run.get(5), graphElement);
    }

    @Test
    public void test_edge_reduce_to_titan() throws IOException {
        HashMap<String, WritableComparable> aliceProp = new HashMap<>();
        aliceProp.put("cf:dept", new StringType("GAO123"));
        aliceProp.put("cf:age", new StringType("43"));
        aliceProp.put("TitanID", new LongType(900L));

        com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType> vertex = newVertex("Alice", aliceProp);

        PropertyMap propertyMap = new PropertyMap();
        propertyMap.setProperty("TitanID", new LongType(903L));
        vertex = newVertex("GAO123", propertyMap);

        vertex = new com.intel.hadoop.graphbuilder.graphelements.Vertex<>(new StringType("GAO123"));
        vertex.setProperty("TitanID", new LongType(903L));

        SerializedGraphElementStringTypeVids elementTwo = new SerializedGraphElementStringTypeVids();
        elementTwo.init(vertex);

        //set up our matching edge to test against
        com.intel.hadoop.graphbuilder.graphelements.Edge<StringType> edge = new com.intel.hadoop.graphbuilder.graphelements.Edge<StringType>(new StringType("Alice"),
                new StringType("GAO123"),
                new StringType("worksAt"));
        edge.setProperty("srcTitanID", new LongType(900L));

        SerializedGraphElementStringTypeVids elementOne = new SerializedGraphElementStringTypeVids();
        elementOne.init(edge);

        SerializedGraphElement[] elements = {elementOne, elementTwo};

        Pair<IntWritable, SerializedGraphElement[]> alice = new Pair<IntWritable, SerializedGraphElement[]>
                (new IntWritable(1), elements);

        Pair<IntWritable, SerializedGraphElement[]>[] pairs = new Pair[]{alice};

        com.tinkerpop.blueprints.Vertex srcBlueprintsVertex = mock(com.tinkerpop.blueprints.Vertex.class);
        com.tinkerpop.blueprints.Vertex dstBlueprintsVertex = mock(com.tinkerpop.blueprints.Vertex.class);
        com.tinkerpop.blueprints.Edge bluePrintsEdge = mock(com.tinkerpop.blueprints.Edge.class);

        PowerMockito.doReturn(srcBlueprintsVertex).when(titanGraph).getVertex(900L);
        PowerMockito.doReturn(dstBlueprintsVertex).when(titanGraph).getVertex(903L);
        PowerMockito.doReturn(bluePrintsEdge).when(titanGraph).addEdge(null, srcBlueprintsVertex, dstBlueprintsVertex, "worksAt");

        runEdgeR(pairs);

        assertEquals("check the number of counters", edgesReduceDriver.getCounters().countCounters(), 1);
        assertEquals("check NUM_EDGES counter", edgesReduceDriver.getCounters().findCounter
                (spiedEdgesIntoTitanReducer.getEdgeCounter()).getValue(), 1);

        verify(titanGraph).getVertex(900L);
        verify(titanGraph).getVertex(903L);
        verify(titanGraph).addEdge(null, srcBlueprintsVertex, dstBlueprintsVertex, "worksAt");
    }

}

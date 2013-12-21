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
package com.intel.hadoop.graphbuilder.pipeline.input;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseTokenizer;
import com.intel.hadoop.graphbuilder.types.StringType;

/**
 * unit test for basemapper with mockito and the PowerMock mockito plugin allows me to mock static methods,
 * private methods and variables.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BaseMapper.class)
public class BaseMapperTest {
    Configuration conf = new Configuration();
    Mapper.Context mapperContextMock;

    Logger loggerMock;
    BaseMapper baseMapper;
    BaseMapper spiedBaseMapper;
    SerializedGraphElement mapVal;

    @BeforeClass
    public static final void beforeClass(){
        //this is to suppress the log 4j errors during the tests
        //we should be moving to the new context logger
        System.setProperty("log4j.ignoreTCL","true");
    }

    @Before
    public void setUp() throws Exception {
        mapVal = new SerializedGraphElementStringTypeVids();

        mapperContextMock = PowerMockito.mock(Mapper.Context.class);

        conf = new Configuration();
        //this is the config if we were loading titan from hbase
        conf.set("GraphTokenizer", HBaseTokenizer.class.getName());
        conf.set("KeyFunction", SourceVertexKeyFunction.class.getName());

        //sample vertex and edge generation rules

        String[] vertexRules = new String[1];
        vertexRules[0] = "cf:name=cf:age,cf:dept";
        HBaseGraphBuildingRule.packVertexRulesIntoConfiguration(conf, vertexRules);

        String[] edgeRules = new String[1];
        edgeRules[0] =  "cf:name,cf:dept,worksAt";
        HBaseGraphBuildingRule.packEdgeRulesIntoConfiguration(conf, edgeRules);

        String[] directedEdgeRules = new String[0];
        HBaseGraphBuildingRule.packDirectedEdgeRulesIntoConfiguration(conf, directedEdgeRules);

        //mapper context mocks
        Class valClass = SerializedGraphElementStringTypeVids.class;
        PowerMockito.when(mapperContextMock.getMapOutputValueClass()).thenReturn(valClass);

        //wil use later to verify log calls
        loggerMock = mock(Logger.class);

        //stub the new BaseMapper(context, conf, log)
        baseMapper = new BaseMapper(mapperContextMock, conf, loggerMock);
        spiedBaseMapper = spy(baseMapper);
        PowerMockito.whenNew(BaseMapper.class).withAnyArguments().thenReturn(spiedBaseMapper);

        spiedBaseMapper.setValClass(SerializedGraphElementStringTypeVids.class);
        //ignore any other calls to set the valClass
        doNothing().when(spiedBaseMapper).setValClass(any(Class.class));

        spiedBaseMapper.setMapVal(SerializedGraphElementStringTypeVids.class.newInstance());
        //ignore any other calls to set map val
        doNothing().when(spiedBaseMapper).setMapVal(any(SerializedGraphElement.class));
    }

    @After
    public void tearDown() {
        mapVal = null;
        mapperContextMock = null;
        conf = null;
        loggerMock = null;
        baseMapper = null;
        spiedBaseMapper = null;
    }


    @Test
    public final void verify_edge_erro_counter_increment() {
        //sample broken edge
        Edge<StringType> brokenEdge = new Edge<StringType>(new StringType("Alice"), new StringType(null),
                new StringType("worksAt"));


        mapVal.init(brokenEdge);

        org.apache.hadoop.mapreduce.Counter EDGE = mock(org.apache.hadoop.mapreduce.Counter.class);

        //mock our context.getCounter() call so context.getCounter().increment(1) don't throw null
        PowerMockito.when(mapperContextMock.getCounter(BaseMapper.getEdgeWriteErrorCounter()))
                .thenReturn(EDGE);

        spiedBaseMapper.incrementErrorCounter(mapperContextMock, mapVal);

        //verify context.getCounter(Counter).increment(1) Edge error Counter got called
        Mockito.verify(EDGE).increment(1);
    }

    @Test
    public final void verify_vertex_error_counter_increment() {
        Vertex<StringType> brokenVertex = new Vertex<StringType>(new StringType("tese"));

        mapVal.init(brokenVertex);

        org.apache.hadoop.mapreduce.Counter VERTEX = mock(org.apache.hadoop.mapreduce.Counter.class);

        //mock our context.getCounter() call so context.getCounter().increment(1) don't throw null
        PowerMockito.when(mapperContextMock.getCounter(BaseMapper.getVertexWriteErrorCounter()))
                .thenReturn(VERTEX);

        spiedBaseMapper.incrementErrorCounter(mapperContextMock, mapVal);

        //verify context.getCounter(Counter).increment(1) vertex error Counter got called
        Mockito.verify(VERTEX).increment(1);
    }

    @Test
    public final void verify_contextWrite_logs_error_and_incrementErrorCounter_on_IOException()
            throws IOException, InterruptedException {
        //mock our exception call
        PowerMockito.doThrow(new IOException()).
                when(mapperContextMock).write(any(IntWritable.class),
                any(SerializedGraphElement.class));

        //this will stop the real call to incrementErrorCounter from happening
        //but the mock will still log the  incrementErrorCounter for later verification
        PowerMockito.doNothing().when(spiedBaseMapper).incrementErrorCounter(any(Mapper.Context.class),
                any(SerializedGraphElement.class));

        //any dummy vertex to call context write
        Vertex<StringType> dummyVertex = new Vertex<StringType>(new StringType("test"));
        mapVal.init(dummyVertex);

        spiedBaseMapper.contextWrite(mapperContextMock, new IntWritable(1), mapVal);

        verify(spiedBaseMapper).incrementErrorCounter(any(Mapper.Context.class),
                any(SerializedGraphElement.class));
        verify(loggerMock).error(any(Object.class), any(IOException.class));
    }

    @Test
    public final void verify_contextWrite_logs_error_and_incrementErrorCounter_on_InterruptedException()
            throws IOException, InterruptedException {

        PowerMockito.doThrow(new InterruptedException()).
                when(mapperContextMock).write(any(IntWritable.class),
                any(SerializedGraphElement.class));

        PowerMockito.doNothing().when(spiedBaseMapper).incrementErrorCounter(any(Mapper.Context.class),
                any(SerializedGraphElement.class));

        Vertex<StringType> dummyVertex = new Vertex<StringType>(new StringType("test"));
        mapVal.init(dummyVertex);

        spiedBaseMapper.contextWrite(mapperContextMock, new IntWritable(1), mapVal);

        verify(spiedBaseMapper).incrementErrorCounter(any(Mapper.Context.class),
                any(SerializedGraphElement.class));
        verify(loggerMock).error(any(Object.class), any(IOException.class));
    }
}

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
package com.intel.hadoop.graphbuilder.pipeline;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.support.membermodification.MemberMatcher.method;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.pipeline.input.BaseMapper;
import com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseReaderMapper;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.EdgesIntoTitanReducer;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanGraphElementWriter;
import com.intel.hadoop.graphbuilder.pipeline.output.titan.VerticesIntoTitanReducer;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseTokenizer;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex;

/**
 * An abstract class that can be extended that will hold most of the testing setup needed for output pipeline
 * greatly reducing the setup needed to test the hbase to vertices to titan mr pipeline and edges to titan reducer.
 * All the external decencies like titan, hbase are mocked out but otherwise this will run the entire pipeline from
 * command line parsing rules, tokenizer to writting to titan. If it's needed to run the MR pipepline it's spied otherwise
 * it's mocked.
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanWriterMRChain
 * @see GBMapReduceDriver
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({EdgesIntoTitanReducer.class, VerticesIntoTitanReducer.class, HBaseReaderMapper.class, TitanGraphElementWriter.class})
public abstract class TestMapReduceDriverUtils {

    protected Configuration conf;
    protected Logger loggerMock;

    protected Mapper.Context mapContext;

    protected HBaseReaderMapper spiedHBaseReaderMapper;
    protected MapDriver<ImmutableBytesWritable, Result, IntWritable, SerializedGraphElement> mapDriver;

    protected VerticesIntoTitanReducer spiedVerticesIntoTitanReducer;
    protected ReduceDriver<IntWritable, SerializedGraphElement, IntWritable, SerializedGraphElement> verticesReduceDriver;

    protected EdgesIntoTitanReducer spiedEdgesIntoTitanReducer;
    protected ReduceDriver<IntWritable, SerializedGraphElement, IntWritable, SerializedGraphElement> edgesReduceDriver;

    protected GBMapReduceDriver<ImmutableBytesWritable, Result, IntWritable, SerializedGraphElement, IntWritable, SerializedGraphElement> gbVertexMapReduceDriver;
    protected GBMapReduceDriver<ImmutableBytesWritable, Result, IntWritable, SerializedGraphElement, IntWritable, SerializedGraphElement> gbEdgeMapReduceDriver;

    protected Mapper.Context mapperContextMock;
    protected Reducer.Context vertexReducerContextMock;
    protected Reducer.Context edgeReducerContextMock;

    protected BaseMapper baseMapper;
    protected BaseMapper spiedBaseMapper;
    //protected PropertyGraphElements propertyGraphElements;
    //protected PropertyGraphElements spiedVertexPropertyGraphElements;
    //protected PropertyGraphElements spiedEdgePropertyGraphElements;
    protected TitanGraphElementWriter titanMergedGraphElementWrite;
    protected TitanGraphElementWriter spiedTitanMergedGraphElementWrite;
    protected TitanGraph titanGraph;

    protected static final Class klass = SerializedGraphElementStringTypeVids.class;
    protected static final Class valClass = SerializedGraphElementStringTypeVids.class;
    protected static final String getTitanGraphInstance = "getTitanGraphInstance";

    @BeforeClass
    public static void beforeClass(){
        //this is to suppress the log 4j errors during the tests
        //we should be moving to the new context logger
        System.setProperty("log4j.ignoreTCL","true");
    }

    @Before
    public void setUp() throws Exception {
        init();
    }

    @After
    public void tearDown(){
        conf = null;
        loggerMock = null;

        mapContext = null;

        spiedHBaseReaderMapper = null;
        mapDriver = null;

        spiedVerticesIntoTitanReducer = null;
        verticesReduceDriver = null;

        spiedEdgesIntoTitanReducer = null;
        edgesReduceDriver = null;

        gbEdgeMapReduceDriver = null;
        gbVertexMapReduceDriver = null;

        mapperContextMock = null;
        vertexReducerContextMock = null;
        edgeReducerContextMock = null;
        baseMapper = null;
        spiedBaseMapper = null;

      /*  propertyGraphElements = null;
        spiedVertexPropertyGraphElements = null;
        spiedEdgePropertyGraphElements = null;*/
        titanMergedGraphElementWrite = null;
        spiedTitanMergedGraphElementWrite = null;
        titanGraph = null;
    }

    /**
     * create a spied instance of EdgesIntoTitanReducer to be used by the EdgesIntoTitanReducer driver
     *
     * @see EdgesIntoTitanReducer
     *
     * @return a new spied EdgesIntoTitanReducer
     */
    protected EdgesIntoTitanReducer newEdgesIntoTitanReducer(){
        spiedEdgesIntoTitanReducer = (EdgesIntoTitanReducer) newSpy(spiedEdgesIntoTitanReducer, EdgesIntoTitanReducer.class);
        try {
            PowerMockito.doReturn(titanGraph).when(spiedEdgesIntoTitanReducer, method(EdgesIntoTitanReducer.class, getTitanGraphInstance, Reducer.Context.class))
                    .withArguments(any(Reducer.Context.class));
        } catch (Exception e) {
            fail("couldn't stub getTitanGraphInstance");
        }
        return spiedEdgesIntoTitanReducer;
    }

    /**
     * create a spied instance of VerticesIntoTitanReducer to be used by the VerticesIntoTitanReducer driver
     *
     * @see VerticesIntoTitanReducer
     *
     * @return a new spied VerticesIntoTitanReducer instance
     */
    protected VerticesIntoTitanReducer newVerticesIntoTitanReducer() {
        spiedVerticesIntoTitanReducer = (VerticesIntoTitanReducer) newSpy(spiedVerticesIntoTitanReducer, VerticesIntoTitanReducer.class);
        try {
            PowerMockito.doReturn(titanGraph).when(spiedVerticesIntoTitanReducer, method(VerticesIntoTitanReducer.class,
                    getTitanGraphInstance, Reducer.Context.class)) .withArguments(any(Reducer.Context.class));
        } catch (Exception e) {
            fail("couldn't stub getTitanGraphInstance");
        }
        return spiedVerticesIntoTitanReducer;
    }

    /**
     * create a spy of the HbaseReaderMapper class that will be used by the HBaseReaderMapper driver
     *
     * @see HBaseReaderMapper
     *
     * @return a new spied HBaseReaderMapper instances
     */
    protected HBaseReaderMapper newHBaseReaderMapper(){
        spiedHBaseReaderMapper = (HBaseReaderMapper) newSpy(spiedHBaseReaderMapper, HBaseReaderMapper.class);
        return spiedHBaseReaderMapper;
    }

    /**
     * create a new EdgesIntoTitanReducer driver and stub the context.getMapOutputValueClass method call. This driver
     * gets used directly it's not used has part of a MapReduceDriver.
     *
     * @see EdgesIntoTitanReducer
     *
     * @return new EdgesIntoTitanReducer driver
     */
    protected ReduceDriver newEdgeReducerDriver(){
        newEdgesIntoTitanReducer();

        edgesReduceDriver = newReduceDriver(spiedEdgesIntoTitanReducer, "edgeReducerContextMock");

        PowerMockito.when(edgeReducerContextMock.getMapOutputValueClass()).thenReturn(klass);

        return edgesReduceDriver;
    }

    /**
     * create a new VerticesIntoTitanReducer driver and stub the context.getMapOutputValueClass method call
     *
     * @see VerticesIntoTitanReducer
     *
     * @return new VerticesIntoTitanReducer driver
     */
    protected ReduceDriver newVerticesIntoTitanReducerDriver() {
        newVerticesIntoTitanReducer();

        verticesReduceDriver = newReduceDriver(spiedVerticesIntoTitanReducer, "vertexReducerContextMock");

        PowerMockito.when(vertexReducerContextMock.getMapOutputValueClass()).thenReturn(klass);

        return verticesReduceDriver;
    }

    /**
     * create our new hbase reader map driver and get the mocked context and stub the context.getMapOutputValueClass
     * method call
     *
     * @see HBaseReaderMapper
     *
     * @return a new HbaseReaderMapper
     */
    protected MapDriver newHbaseReaderMapperDriver(){
        newHBaseReaderMapper();

        mapDriver = MapDriver.newMapDriver(spiedHBaseReaderMapper);

        mapContext =  mapDriver.getContext();

        PowerMockito.when(mapContext.getMapOutputValueClass()).thenReturn(klass);

        return mapDriver;
    }

    /**
     * Run prerequisites needed for the hbase->vertex map reduce driver and create our new MapReduceDriver
     *
     * @see GBMapReduceDriver
     * @see HBaseReaderMapper
     * @see VerticesIntoTitanReducer
     *
     * @return a new Hbase->vertices GBMapReduceDriver
     */
    protected MapReduceDriver newVertexHbaseMR(){
        newVerticesIntoTitanReducer();
        newHBaseReaderMapper();
        newConfiguration();

        gbVertexMapReduceDriver = new GBMapReduceDriver(mapDriver, verticesReduceDriver);

        return gbVertexMapReduceDriver;
    }

    /**
     * abstract the setup and running of MapReduceDriver for any other Hbase->? jobs we might have
     *
     * @see GBMapReduceDriver
     *
     * @param mapReduceDriver the GBMapReduceDriver
     * @param pairs row key, column hbase data
     * @return any reducer context.write output
     * @throws IOException
     */
    protected List<Pair<IntWritable,SerializedGraphElement>> runMapReduceDriver( GBMapReduceDriver mapReduceDriver,
            Pair<ImmutableBytesWritable,Result>[] pairs) throws IOException {

        mapReduceDriver.withConfiguration(conf);

        for(Pair<ImmutableBytesWritable, Result> kv: pairs){
            mapReduceDriver.withInput(kv);
        }

        return mapReduceDriver.run();
    }

    /**
     * takes row key, hbase Result pairs to run hbase->vertices MR job. Will also take care of adding the hadoop
     * configuration to the driver.
     *
     * @see HBaseReaderMapper
     * @see VerticesIntoTitanReducer
     *
     * @param pairs row key, hbase Result pairs to pass to the hbase->vertices MR job
     * @return any context.write output from the reducer
     * @throws IOException
     */
    protected List<Pair<IntWritable,SerializedGraphElement>> runVertexHbaseMR(
            Pair<ImmutableBytesWritable,Result>[] pairs) throws IOException {

        return runMapReduceDriver(gbVertexMapReduceDriver, pairs);
    }

    /**
     * add the hadoop config and inputs to the edge reduce driver and return the context.write output.
     *
     * @param pairs a Key value pair data to be used to test the Reducer
     * @return any context.write output
     * @throws IOException
     */
    protected List<Pair<IntWritable,SerializedGraphElement>> runEdgeR(
            Pair<IntWritable, SerializedGraphElement[]>[] pairs) throws IOException{

        edgesReduceDriver.withConfiguration(conf);

        for(Pair<IntWritable, SerializedGraphElement[]> kv: pairs){
            edgesReduceDriver.withInput(kv.getFirst(), Arrays.asList(kv.getSecond()) );
        }

        return edgesReduceDriver.run();
    }

    /**
     * mock and set our logger into HbaseReaderMapper to verify the logging of error messages.
     * @return new Logger mock
     */
    protected Logger newLoggerMock(){
        if( loggerMock == null){
            loggerMock = mock(Logger.class);
            Whitebox.setInternalState(HBaseReaderMapper.class, "LOG", loggerMock);
        }
        return loggerMock;
    }

    /**
     * a real hbase configuration we use with the map/reduce pipeline with the minimum set to get it working
     *
     * @return a new hbase configuration
     */
    protected Configuration newConfiguration(){
        if(conf == null){
            conf = new Configuration();
            conf.set("GraphTokenizer", HBaseTokenizer.class.getName());
            conf.set("KeyFunction", SourceVertexKeyFunction.class.getName());
        }
        return conf;
    }

    /**
     * The graph building rules that are usually defined on the command line minus the command line option and broken out
     * into string arrays if you have more than one per option
     */
    protected void newGraphBuildingRules(){
        //sample vertex and edge generation rules
        String[] vertexRules = new String[1];
        vertexRules[0] = "cf:name=cf:age,cf:dept";
        HBaseGraphBuildingRule.packVertexRulesIntoConfiguration(conf, vertexRules);

        String[] edgeRules = new String[0];
        HBaseGraphBuildingRule.packEdgeRulesIntoConfiguration(conf, edgeRules);

        String[] directedEdgeRules = new String[1];
        directedEdgeRules[0] =  "cf:name,cf:dept,worksAt";
        HBaseGraphBuildingRule.packDirectedEdgeRulesIntoConfiguration(conf, directedEdgeRules);
    }

    /**
     * mock the mapper context so we can return a getMapOutputValueClass
     * @return new mocked mapper.context
     */
    protected Mapper.Context newMapperContext(){
        if(mapperContextMock == null){
            mapperContextMock = mock(Mapper.Context.class);
            PowerMockito.when(mapperContextMock.getMapOutputValueClass()).thenReturn(valClass);
        }
        return mapperContextMock;
    }

    /**
     *
     * @see BaseMapper
     *
     * @return a spied BaseMapper
     * @throws Exception
     */
    protected BaseMapper newBaseMapper() throws Exception {
        if(spiedBaseMapper == null){
            baseMapper = new BaseMapper(mapperContextMock, conf, loggerMock);
            spiedBaseMapper = spy(baseMapper);
            PowerMockito.whenNew(BaseMapper.class).withAnyArguments().thenReturn(spiedBaseMapper);
        }
        return spiedBaseMapper;
    }

    /**
     * spy on our merge duplicate interface that does the context write.
     *
     * @see com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanGraphElementWriter
     * @return new spied TitanGraphElementWriter
     */
    protected TitanGraphElementWriter newTitanMergedGraphElementWrite() throws Exception {
        spiedTitanMergedGraphElementWrite = (TitanGraphElementWriter)newSpy(spiedTitanMergedGraphElementWrite,
                TitanGraphElementWriter.class);

        PowerMockito.whenNew(TitanGraphElementWriter.class).withAnyArguments().thenReturn
                (spiedTitanMergedGraphElementWrite);

        return spiedTitanMergedGraphElementWrite;
    }


    /**
     * create a spied PropertyGraphElements to stub out methods later in our test
     *
     * @see PropertyGraphElements
     *
     * @throws Exception
     *//*
    protected void newPropertyGraphElements() throws Exception {
        //step some mocks that get set when we create our spy
        newVerticesIntoTitanReducer();
        newTitanMergedGraphElementWrite();

        if(spiedVertexPropertyGraphElements == null){

            spiedVertexPropertyGraphElements = spy(new PropertyGraphElements(spiedTitanMergedGraphElementWrite, null, null,
                    vertexReducerContextMock, titanGraph,
                    (SerializedGraphElement)valClass.newInstance(), spiedVerticesIntoTitanReducer.getEdgeCounter(),
                    spiedVerticesIntoTitanReducer.getVertexCounter()));

            *//**
             * this will make sure our spied instance get returned when it's instantiated in the
             * VerticesIntoTitanReducer.initPropertyGraphElements
             * @see VerticesIntoTitanReducer
             *//*
            PowerMockito.whenNew(PropertyGraphElements.class).withAnyArguments().thenReturn(spiedVertexPropertyGraphElements);
        }
    }*/

    /**
     * create our titan graph mock and assign it to our class a titanGraph class field and return.
     *
     * @see TitanGraph
     *
     * @return a new mocked titan graph
     */
    protected TitanGraph newTitanGraphMock(){
        titanGraph = (TitanGraph) newMock(titanGraph, TitanGraph.class);
        return titanGraph;
    }

    /**
     * Create a new reduce driver with the context mocked
     *
     * @param reducer the hadoop reducer we are going to create the driver for
     * @param contextFieldName this clases's context field name so we can assign the mocked context
     * @return a new reduce driver
     */
    protected ReduceDriver newReduceDriver(org.apache.hadoop.mapreduce.Reducer reducer, String contextFieldName){
        ReduceDriver newDriver = ReduceDriver.newReduceDriver(reducer);

        Field field;

        try {
            field = TestMapReduceDriverUtils.class.getDeclaredField(contextFieldName);
            field.setAccessible(true);
            try {
                field.set(this, newDriver.getContext());
            } catch (IllegalAccessException e) {
                fail("couldn't set context");
            }
        } catch (NoSuchFieldException e) {
            fail("couldn't find context field: " + contextFieldName);
        }

        return newDriver;
    }

    /**
     * create a new mock if it's not already created
     *
     * @param inst the object that will hold the mock
     * @param klass the class we are mocking
     * @return new mock
     */
    protected Object newMock(Object inst, Class klass){
        if(inst == null){
            inst = mock(klass);
        }
        return inst;
    }

    /**
     * create new spied instances if they are not set.
     *
     * @param object the object that will hold the spied intance
     * @param klass the class we are going to create a spy for
     * @return the spied instance
     */
    protected Object newSpy(Object object, Class klass){
        if(object == null){
            try {
                object = klass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                //if we get any of the thrown exceptions fail the test because nothing is going to work
                fail("couldn't spy: " + klass.getName());
            }
            object = spy(object);
        }
        return object;
    }

    public com.tinkerpop.blueprints.Vertex vertexMock(){
        return mock(StandardVertex.class);
    }


    /**
     * setup our testing environment. is called during the setUp
     *
     * @throws Exception
     */
    protected void init() throws Exception {
        newTitanGraphMock();

        newEdgeReducerDriver();
        newVerticesIntoTitanReducerDriver();

        newTitanMergedGraphElementWrite();

        newHbaseReaderMapperDriver();
        newVertexHbaseMR();

        newLoggerMock();
        newConfiguration();
        newGraphBuildingRules();
        newMapperContext();

        newBaseMapper();
    }

    /**
     * small helper function to help with the creation of vertices.
     *
     * @see Vertex
     *
     * @param vertexId string vertex id. will be used to create the StringType vertex id
     * @param properties a hash map with all the properties
     * @return new StringType vertex
     */
    public static final Vertex<StringType> newVertex(String vertexId, HashMap<String, WritableComparable> properties){
        com.intel.hadoop.graphbuilder.graphelements.Vertex vertex =
                new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType(vertexId));
        for(Map.Entry<String, WritableComparable> entry: properties.entrySet()){
            vertex.setProperty(entry.getKey(), entry.getValue());
        }
        return vertex;
    }

    /**
     * small helper function to help with the creation of vertices.
     *
     * @see Vertex
     *
     * @param vertexId string vertex id. will be used to create the StringType vertex id
     * @param propertyMap a new property map for the vertex properties
     * @return new StringType vertex
     */
    public static final Vertex<StringType> newVertex(String vertexId, PropertyMap propertyMap){
        com.intel.hadoop.graphbuilder.graphelements.Vertex vertex =
                new com.intel.hadoop.graphbuilder.graphelements.Vertex<StringType>(new StringType(vertexId), propertyMap);
        return vertex;
    }

    /**
     * small helper function to help with the creation of edges.
     *
     * @see Edge
     *
     * @param src string edge src. will be used to create the new StringType
     * @param dst string edge dst. will be used to create the new StringType
     * @param label string edge label. will be used to create the new StringType
     * @param propertyMap  a new property map for the edge properties
     * @return new StringType edge
     */
    public static final Edge<StringType> newEdge(String src, String dst, String label, PropertyMap propertyMap){
        com.intel.hadoop.graphbuilder.graphelements.Edge edge =
                new Edge<StringType>(new StringType(src), new StringType(dst), new StringType(label), propertyMap);;
        return edge;
    }

    /**
     * small helper function to help with the creation of edges.
     *
     * @see Edge
     *
     * @param src string edge src. will be used to create the new StringType
     * @param dst string edge dst. will be used to create the new StringType
     * @param label string edge label. will be used to create the new StringType
     * @param properties hashmap with all the desired edge properties
     * @return new StringType Edge
     */
    public static final Edge<StringType> newEdge(String src, String dst, String label, HashMap<String, WritableComparable> properties){
        com.intel.hadoop.graphbuilder.graphelements.Edge edge =
                new Edge<StringType>(new StringType(src), new StringType(dst), new StringType(label));
        for(Map.Entry<String, WritableComparable> entry: properties.entrySet()){
            edge.setProperty(entry.getKey(), entry.getValue());
        }
        return edge;
    }

    /**
     * setup our sample data for our Results column list
     *
     * @return Result column list
     */
    public static final Result sampleDataAlice() {
        ArrayList<KeyValue> list = new ArrayList<KeyValue>();
        //alice
        list.add(newKeyValue("row1", "cf", "age", "43", "1381447886360"));
        list.add(newKeyValue("row1", "cf", "dept", "GAO123", "1381447886375"));
        list.add(newKeyValue("row1", "cf", "id", "0001", "1381447886305"));
        list.add(newKeyValue("row1", "cf", "manager", "Zed", "1381447886386"));
        list.add(newKeyValue("row1", "cf", "name", "Alice", "1381447886328"));
        list.add(newKeyValue("row1", "cf", "underManager", "5yrs", "1381447886400"));

        return new Result(list);
    }

    /**
     * setup our sample data for our Results column list
     *
     * @return Result column list
     */
    public static final Result sampleDataBob() {
        ArrayList<KeyValue> list = new ArrayList<KeyValue>();
        //alice
        list.add(newKeyValue("row2", "cf", "age", "45", "1381447886360"));
        list.add(newKeyValue("row2", "cf", "dept", "INTELLABS", "1381447886375"));
        list.add(newKeyValue("row2", "cf", "id", "00002", "1381447886305"));
        list.add(newKeyValue("row2", "cf", "manager", "Zed", "1381447886386"));
        list.add(newKeyValue("row2", "cf", "name", "Bob", "1381447886328"));
        list.add(newKeyValue("row2", "cf", "underManager", "1yrs", "1381447886400"));

        return new Result(list);
    }

    /**
     * help debug the wierdness with Hbase.Client.Results. prints all the sample data to see if any values are coming
     * back null
     *
     * @throws UnsupportedEncodingException
     */
    public final void printSampleData(Result result) throws UnsupportedEncodingException {
        printSampleRow(result, "cf", "age");
        printSampleRow(result, "cf", "dept");
        printSampleRow(result, "cf", "id");
        printSampleRow(result, "cf", "manager");
        printSampleRow(result, "cf", "name");
        printSampleRow(result, "cf", "underManager");
    }

    /**
     * will print the value for the given Column faimily for the hbase Result set. Will convert all CF and Qualifier strings
     * to bytes with HConstants.UTF8_ENCODING before trying to read the qualifier value.
     *
     * @param result hbase result
     * @param cf hbase column family
     * @param qualifier hbase qualifier
     * @throws UnsupportedEncodingException
     */
    public final void printSampleRow(Result result, String cf, String qualifier) throws UnsupportedEncodingException {
        System.out.println((cf + ":" + qualifier + " value: " +
                Bytes.toString(result.getValue(cf.getBytes(HConstants.UTF8_ENCODING),
                        qualifier.getBytes(HConstants.UTF8_ENCODING)))));
    }

    /**
     * small wrapper method to help us create KeyValues. All the input values are strings that will be converted to
     * bytes with String.getBytes()
     *
     * @param row       the row key string
     * @param cf        the column family string
     * @param qualifier the qualifier string
     * @param value     the value string
     * @param time      the timestamp for the row as a string
     * @return
     */
    public static final KeyValue newKeyValue(String row, String cf, String qualifier, String value, String time) {
        KeyValue k1;
        try {
            k1 = new KeyValue(row.getBytes(HConstants.UTF8_ENCODING),
                    cf.getBytes(HConstants.UTF8_ENCODING),
                    qualifier.getBytes(HConstants.UTF8_ENCODING), Long.valueOf(time), KeyValue.Type.Put,
                    value.getBytes(HConstants.UTF8_ENCODING));
        } catch (UnsupportedEncodingException e) {
            return null;
        }
        return k1;
    }

    /**
     * verify the value of the context.write(key, value). we don't care about the int writable because it's a key and
     * will change no matter what our input data is
     *
     * @param pair             the writable pair from the mapper
     *
     */
    public final void verifyPairSecond(Pair<IntWritable, SerializedGraphElement> pair,
                                       SerializedGraphElement graphElement){

        //checking the graphElement type of pair and graphElement i'm verifying against
        assertEquals("graph types must match", pair.getSecond().graphElement().getType(),
                graphElement.graphElement().getType());

        //assign to local variables to reduce line length;
        GraphElement jobGraphElement = pair.getSecond().graphElement();
        GraphElement givenGraphElement = graphElement.graphElement();

        /*i'm not making any distinction between vertex and edge. it makes the code cleaner i just have to watch out for
            nulls. verify all the edge, vertex values match
        */
        assertTrue("match src", (jobGraphElement.getSrc() == null && givenGraphElement.getSrc() == null)
                || jobGraphElement.getSrc().equals(givenGraphElement.getSrc()));
        assertTrue("match dst", (jobGraphElement.getDst() == null && givenGraphElement.getDst() == null)
                || jobGraphElement.getDst().equals(givenGraphElement.getDst()));
        assertTrue("match label", (jobGraphElement.getLabel() == null && givenGraphElement.getLabel() == null)
                || jobGraphElement.getLabel().equals(givenGraphElement.getLabel()));
        assertTrue("match id", (jobGraphElement.getId() == null && givenGraphElement.getId() == null)
                || jobGraphElement.getId().equals(givenGraphElement.getId()));

        //verify all the edge/vertex properties match
        for (Writable writable : jobGraphElement.getProperties().getPropertyKeys()) {
            String key = ((StringType) writable).get();
            Object value = jobGraphElement.getProperty(key);
            assertTrue(String.format("Look for %s:%s pair in our baseline object ", key, value.toString()),
                    givenGraphElement.getProperty(key).equals(value));
        }
    }
}

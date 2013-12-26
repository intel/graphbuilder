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

import com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.output.GraphGenerationMRJob;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.EdgeSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertyGraphSchema;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema.PropertySchema;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.util.*;
import com.intel.hadoop.graphbuilder.util.Timer;
import com.thinkaurelius.titan.core.KeyMaker;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanKey;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * This class handles loading the constructed property graph into Titan.
 *
 * <p>There is some configuration of Titan at setup time,
 * followed by  a chain of  two map reduce jobs.
 * </p>
 *
 * <p>
 * At setup time, this class makes a connection to Titan,
 * and declares all necessary keys, properties and edge signatures.
 * </p>
 * <p>
 * The first mapper, which generates a multiset of property graph elements,
 * is determined by the input configuration and the graph building rule.
 * These two objects are parameters to the {@code init} method.
 * </p>
 *
 * <p>
 * At the first reducer, the vertices are gathered by the hash of their IDs, and 
 * the edges are gathered by the hashes of their source vertex IDs, 
 * see {@code SourceVertexKeyFunction}.
 * </p>
 *
 * The first reducer performs the following tasks:
 * <ul>
 *   <li> Removes any duplicate edges or vertices. 
 *       (By default, the class combines the property maps for duplicates).</li>
 *   <li> Stores the vertices in Titan.</li>
 *   <li> Creates a temporary HDFS file containing the property graph elements 
 *        annotated as follows:
 *   <ul>
 *       <li>Vertices are annotated with their Titan IDs.</li>
 *       <li>Edges are annotated with the Titan IDs of their source vertexes
 *       .</li>
 *       </ul>
 *       </ul>
 *
 * <p>
 * At the second reducer, the vertices are gathered by the hashes of their 
 * IDs, and edges are gathered by the hashes of their destination vertex IDs, 
 * see {@code DestinationVertexKeyFunction}.
 *  </p>
 * <p>
 * The second reducer then loads the edges into Titan.
 * </p>
 *
 * @see InputConfiguration
 * @see GraphBuildingRule
 * @see SourceVertexKeyFunction
 * @see com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction
 * .DestinationVertexKeyFunction
 */

public class TitanWriterMRChain extends GraphGenerationMRJob  {

    private static final Logger LOG =
            Logger.getLogger(TitanWriterMRChain.class);
    private static final int RANDOM_MIN = 1;
    private static final int RANDOM_MAX = 1000000;
    private Configuration    conf;

    private HBaseUtils hbaseUtils = null;
    private boolean    usingHBase = false;

    private GraphBuildingRule  graphBuildingRule;
    private InputConfiguration inputConfiguration;

    private SerializedGraphElement mapValueType;
    private Class                vidClass;
    private PropertyGraphSchema  graphSchema;

    private Functional vertexReducerFunction;
    private Functional edgeReducerFunction;
    private boolean    cleanBidirectionalEdge;


    /**
     * Acquires the set-up time components necessary for creating a graph from  
	 * the raw data and loading it into Titan.
     * @param  {@code inputConfiguration}  The object that handles the creation 
	 *                                     of data records from raw data.
     * @param  {@code graphBuildingRule}   The object that handles the creation 
	 *                                     of property graph elements from
     *                                     data records.
     *
     *
     */

    @Override
    public void init(InputConfiguration inputConfiguration,
                     GraphBuildingRule graphBuildingRule) {

        this.graphBuildingRule  = graphBuildingRule;
        this.inputConfiguration = inputConfiguration;
        this.graphSchema        = graphBuildingRule.getGraphSchema();
        this.usingHBase         = true;

        try {
            this.hbaseUtils = HBaseUtils.getInstance();
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode
                    .UNABLE_TO_CONNECT_TO_HBASE,
                    "GRAPHBUILDER_ERROR: Cannot allocate the HBaseUtils " +
                            "object. Check hbase connection.", LOG, e);
        }

        this.conf = hbaseUtils.getConfiguration();

    }

    /**
     * Sets the user defined functions to reduce duplicate vertices and edges.
     *
     * If the user does not specify these functions, the default method of  
	 * merging property lists will be used.
     *
     * @param {@code vertexReducerFunction}   The user specified function for 
	 *                                        reducing duplicate vertices.
     * @param {@code edgeReducerFunction}     The user specified function for 
	 *                                        reducing duplicate edges.
     */

    public void setFunctionClass(Class vertexReducerFunction,
                                 Class edgeReducerFunction) {
        try {
            if (vertexReducerFunction != null) {
                this.vertexReducerFunction = (Functional)
                        vertexReducerFunction.newInstance();
            }

            if (edgeReducerFunction != null) {
                this.edgeReducerFunction = (Functional) edgeReducerFunction
                        .newInstance();
            }
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode
                    .CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Unable to instantiate reducer " +
                            "functions.", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode
                    .CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Illegal access exception when " +
                            "instantiating reducer functions.", LOG, e);
        }
    }

    /**
     * Sets the option to clean bidirectional edges.
     *
     * @param {@code true}  The boolean option value, if true then remove 
	 *                      bidirectional edges.
     */

    @Override
    public void setCleanBidirectionalEdges(boolean clean) {
        cleanBidirectionalEdge = clean;
    }

    /**
     * Sets the value class for the property graph elements coming from the 
	 * mapper or tokenizer.
     *
     * This type can vary depending on the class used for vertex IDs.
     *
     * @param {@code valueClass}  The class of the {@code
     * SerializedGraphElement} value.
     * @see com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement
     * @see com.intel.hadoop.graphbuilder.graphelements
     * .SerializedGraphElementLongTypeVids
     * @see com.intel.hadoop.graphbuilder.graphelements
     * .SerializedGraphElementStringTypeVids
     */

    @Override
    public void setValueClass(Class valueClass) {
        try {
            this.mapValueType = (SerializedGraphElement) valueClass
                    .newInstance();
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode
                    .CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Cannot set value class ( " +
                            valueClass.getName() + ")", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode
                    .CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Illegal access exception when " +
                            "setting value class ( " + valueClass.getName() +
                            ")", LOG, e);
        }
    }

    /**
     * Sets the vertex id class.
     *
     * Currently long and String are supported.
     * @see com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement
     * @see com.intel.hadoop.graphbuilder.graphelements
     * .SerializedGraphElementLongTypeVids
     * @see com.intel.hadoop.graphbuilder.graphelements
     * .SerializedGraphElementStringTypeVids
     */

    @Override
    public void setVidClass(Class vidClass) {
        this.vidClass = vidClass;
    }

    /**
     * @return The configuration of the current job.
     */

    public Configuration getConf() {
        return this.conf;
    }


    private Integer random(){
        Random rand = new Random();

        int randomNum = rand.nextInt((RANDOM_MAX - RANDOM_MIN) + 1) +
                RANDOM_MIN;

        return randomNum;
    }

    /**
     * Sets the user defined options.
     *
     * @param {@code userOpts} A Map of option key and value pairs.
     */
    @Override
    public void setUserOptions(HashMap<String, String> userOpts) {
        Set<String> keys = userOpts.keySet();
        for (String key : keys)
            conf.set(key, userOpts.get(key.toString()));
    }


    /**
     * Creates the Titan graph for saving edges and removes the static open
     * method from setup so it can be mocked-up.
     *
     * @return {@code TitanGraph}  For saving edges.
     * @throws IOException
     */
    private TitanGraph getTitanGraphInstance(Configuration configuration)
            throws IOException {
        BaseConfiguration titanConfig = new BaseConfiguration();

        return GraphDatabaseConnector.open("titan", titanConfig, configuration);
    }

    /*
     * This private method does the parsing of the command line -keys option
     * into a list of GBTitanKey objects.
     *
     * The -keys option takes a comma separated list of key rules.
     *
     * A key rule is:  <property name>;<option_1>; ... <option_n>
     * where the options are datatype specifiers, flags to use the key for
     * indexing edges and vertices, or a uniqueness bit,
     * per the definitions in TitanCommandLineOptions.
     *
     * Example:
     *    -keys  cf:userId;String;U;V,cf:eventId;E;Long
     *
     *    Generates a key for property cf:UserId that is a unique vertex
     *    index taking string values, and a key for property cf:eventId that
     *    is an edge index taking Long values.
     */
    private List<GBTitanKey> parseKeyCommandLine(String keyCommandLine) {

        ArrayList<GBTitanKey> gbKeyList = new ArrayList<GBTitanKey>();

        if (keyCommandLine.length() > 0) {

            String[] keyRules = keyCommandLine.split("\\,");

            for (String keyRule : keyRules) {
                String[] ruleProperties = keyRule.split(";");

                if (ruleProperties.length > 0) {
                    String propertyName = ruleProperties[0];

                    GBTitanKey gbTitanKey = new GBTitanKey(propertyName);

                    for (int i = 1; i < ruleProperties.length; i++) {
                        String ruleModifier = ruleProperties[i];

                        if (ruleModifier.equals(TitanCommandLineOptions
                                .STRING_DATATYPE)) {
                            gbTitanKey.setDataType(String.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.INT_DATATYPE)) {
                            gbTitanKey.setDataType(Integer.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.LONG_DATATYPE)) {
                            gbTitanKey.setDataType(Long.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.DOUBLE_DATATYPE)) {
                            gbTitanKey.setDataType(Double.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.FLOAT_DATATYPE)) {
                            gbTitanKey.setDataType(Float.class);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.VERTEX_INDEXING)) {
                            gbTitanKey.setIsVertexIndex(true);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.EDGE_INDEXING)) {
                            gbTitanKey.setIsEdgeIndex(true);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.UNIQUE)) {
                            gbTitanKey.setIsUnique(true);
                        } else if (ruleModifier.equals
                                (TitanCommandLineOptions.NOT_UNIQUE)) {
                            gbTitanKey.setIsUnique(false);
                        } else {
                            GraphBuilderExit.graphbuilderFatalExitNoException
                                    (StatusCode.BAD_COMMAND_LINE,
                                    "Error declaring keys.  " + ruleModifier
                                            + " is not a valid option.\n" +
                                            TitanCommandLineOptions
                                                    .KEY_DECLARATION_CLI_HELP,
                                            LOG);
                        }
                    }

                    // Titan requires that unique properties be vertex indexed

                    if (gbTitanKey.isUnique()) {
                        gbTitanKey.setIsVertexIndex(true);
                    }

                    gbKeyList.add(gbTitanKey);
                }
            }
        }

        return gbKeyList;
    }

    /*
     * Gets the set of Titan Key definitions from the command line...
     */

    private HashMap<String, TitanKey>
        declareAndCollectKeys(TitanGraph graph, String keyCommandLine) {

        HashMap<String, TitanKey> keyMap = new HashMap<String, TitanKey>();

        TitanKey gbIdKey = null;

        // Because Titan requires combination of vertex names and vertex
        // labels into single strings for unique IDs the unique
        // GB_ID_FOR_TITAN property must be of StringType

        gbIdKey = graph.makeKey(TitanConfig.GB_ID_FOR_TITAN).dataType(String
                .class)
                .indexed(Vertex.class).unique().make();


        keyMap.put(TitanConfig.GB_ID_FOR_TITAN, gbIdKey);

        List<GBTitanKey> declaredKeys = parseKeyCommandLine(keyCommandLine);

        for (GBTitanKey gbTitanKey : declaredKeys) {
            KeyMaker keyMaker = graph.makeKey(gbTitanKey.getName());
            keyMaker.dataType(gbTitanKey.getDataType());

            if (gbTitanKey.isEdgeIndex()) {
                keyMaker.indexed(Edge.class);
            }

            if (gbTitanKey.isVertexIndex()) {
                keyMaker.indexed(Vertex.class);
            }

            if (gbTitanKey.isUnique()) {
                keyMaker.unique();
            }

            TitanKey titanKey = keyMaker.make();

            keyMap.put(titanKey.getName(), titanKey);
        }

        HashMap<String, Class<?>> propertyNameToTypeMap = graphSchema
                .getMapOfPropertyNamesToDataTypes();

        for (String property : propertyNameToTypeMap.keySet()) {

            if (!keyMap.containsKey(property)) {
                TitanKey key = graph.makeKey(property).dataType
                        (propertyNameToTypeMap.get(property)).make();
                keyMap.put(property, key);
            }

        }

        return keyMap;
    }

    /*
     * Opens the Titan graph database, and make the Titan keys required by
     * the graph schema.
     */
    private void initTitanGraph (String keyCommandLine) {
        TitanGraph graph = null;

        try {
            graph = getTitanGraphInstance(conf);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode
                    .UNHANDLED_IO_EXCEPTION,
                    "GRAPHBUILDER FAILURE: Unhandled IO exception while " +
                            "attempting to connect to Titan.",  LOG, e);
        }

        HashMap<String, TitanKey> propertyNamesToTitanKeysMap =
                declareAndCollectKeys(graph, keyCommandLine);

        // now we declare the edge labels
        // one of these days we'll probably want to fully expose all the
        // Titan knobs regarding manyToOne, oneToMany, etc



        for (EdgeSchema edgeSchema : graphSchema.getEdgeSchemata())  {
            ArrayList<TitanKey> titanKeys = new ArrayList<TitanKey>();

            for (PropertySchema propertySchema : edgeSchema
                    .getPropertySchemata() ) {
                titanKeys.add(propertyNamesToTitanKeysMap.get(propertySchema
                        .getName()));
            }

            TitanKey[] titanKeyArray = titanKeys.toArray(new
                    TitanKey[titanKeys.size()]);
            graph.makeLabel(edgeSchema.getLabel()).signature(titanKeyArray)
                    .make();
        }

        graph.commit();
    }

    /**
     * Executes the MR chain that constructs a graph from the raw input
     * specified by
     * {@code InputConfiguration} according to the graph construction rule
     * {@code GraphBuildingRule},
     * and loads it into the Titan graph database.
     *
     * @param {@code cmd}  User specified command line.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void run(CommandLine cmd)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Warns the user if the Titan table already exists in Hbase.

        String titanTableName = TitanConfig.config.getProperty
                ("TITAN_STORAGE_TABLENAME");

        if (hbaseUtils.tableExists(titanTableName)) {
            if (cmd.hasOption(BaseCLI.Options.titanAppend.getLongOpt())) {
                LOG.info("WARNING:  hbase table " + titanTableName +
                         " already exists. Titan will append new graph to " +
                        "existing data.");
            } else if (cmd.hasOption(BaseCLI.Options.titanOverwrite
                    .getLongOpt())) {
                HBaseUtils.getInstance().removeTable(titanTableName);
                LOG.info("WARNING:  hbase table " + titanTableName +
                        " already exists. Titan will overwrite existing data " +
                        "with the new graph.");
            } else {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode
                        .BAD_COMMAND_LINE,
                        "GRAPHBUILDER_FAILURE: hbase table " + titanTableName +
                                " already exists. Use -a option if you wish " +
                                "to append new graph to existing data."
                        + " Use -O option if you wish to overwrite the graph" +
                                ".", LOG);
            }
        }

        String intermediateDataFileName = "graphElements-" + random()
                .toString();
        Path   intermediateDataFilePath =
                new Path("/tmp/graphbuilder/" +  intermediateDataFileName);

        String intermediateEdgeFileName = "labeledEdges-" + random().toString();
        Path   intermediateEdgeFilePath =
                new Path("/tmp/graphbuilder/" + intermediateEdgeFileName);

        String keyCommandLine = new String("");

        Timer time = new Timer();
        time.start();
        if (cmd.hasOption(BaseCLI.Options.titanKeyIndex.getLongOpt())) {
            keyCommandLine = cmd.getOptionValue(BaseCLI.Options.titanKeyIndex
                    .getLongOpt());
        }

        initTitanGraph(keyCommandLine);

        runReadInputLoadVerticesMRJob(intermediateDataFilePath, cmd);

        runIntermediateEdgeWriteMRJob(intermediateDataFilePath,
                intermediateEdgeFilePath);
        runEdgeLoadMapOnlyJob(intermediateEdgeFilePath);

        Long runtime = time.time_since_last();
        LOG.info("Time taken to load the graph to Titan: "
                + runtime +  " seconds");

        FileSystem fs = FileSystem.get(getConf());
        fs.delete(intermediateDataFilePath, true);
        fs.delete(intermediateEdgeFilePath, true);
    }

    private void runReadInputLoadVerticesMRJob(Path intermediateDataFilePath,
                                               CommandLine cmd)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Set required parameters in configuration

        conf.set("GraphTokenizer", graphBuildingRule.getGraphTokenizerClass()
                .getName());
        conf.setBoolean("noBiDir", cleanBidirectionalEdge);
        conf.set("vidClass", vidClass.getName());
        conf.set("KeyFunction", SourceVertexKeyFunction.class.getName());

        // Set optional parameters in configuration

        if (vertexReducerFunction != null) {
            conf.set("vertexReducerFunction", vertexReducerFunction.getClass
                    ().getName());
        }
        if (edgeReducerFunction != null) {
            conf.set("edgeReducerFunction", edgeReducerFunction.getClass()
                    .getName());
        }

        // set the configuration per the input

        inputConfiguration.updateConfigurationForMapper(conf);

        // update the configuration per the tokenizer

        graphBuildingRule.updateConfigurationForTokenizer(conf);

        // create loadVerticesJob from configuration and initialize MR
        // parameters

        Job loadVerticesJob = new Job(conf, "TitanWriterMRChain Job 1: " +
                "Writing Vertices into Titan");
        loadVerticesJob.setJarByClass(TitanWriterMRChain.class);

        // configure mapper  and input

        inputConfiguration.updateJobForMapper(loadVerticesJob);

        loadVerticesJob.setMapOutputKeyClass(IntWritable.class);
        loadVerticesJob.setMapOutputValueClass(mapValueType.getClass());

        // configure reducer  output

        loadVerticesJob.setReducerClass(VerticesIntoTitanReducer.class);


        // check that the graph database is up and running...
        GraphDatabaseConnector.checkTitanInstallation();

        // now we set up those temporary storage locations...;

        loadVerticesJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        loadVerticesJob.setOutputKeyClass(IntWritable.class);
        loadVerticesJob.setOutputValueClass(mapValueType.getClass());

        FileOutputFormat.setOutputPath(loadVerticesJob, intermediateDataFilePath);

        // fired up and ready to go!

        LOG.info("=========== TitanWriterMRChain Job 1: Create initial  graph" +
                " elements from raw data, load vertices to Titan ===========");

        LOG.info("input: " + inputConfiguration.getDescription());
        LOG.info("Output is a titan dbase = " +  TitanConfig.config
                .getProperty("TITAN_STORAGE_TABLENAME"));

        LOG.info("InputFormat = " + inputConfiguration.getDescription());
        LOG.info("GraphBuildingRule = " + graphBuildingRule.getClass()
                .getName());

        if (vertexReducerFunction != null) {
            LOG.info("vertexReducerFunction = " + vertexReducerFunction
                    .getClass().getName());
        }

        if (edgeReducerFunction != null) {
            LOG.info("edgeReducerFunction = " + edgeReducerFunction.getClass
                    ().getName());
        }

        LOG.info("==================== Start " +
                "====================================");
        loadVerticesJob.waitForCompletion(true);
        LOG.info("=================== Done " +
                "====================================\n");

    }

    private void runIntermediateEdgeWriteMRJob(
            Path intermediateDataFilePath,
            Path intermediateEdgeFilePath) throws
            IOException, ClassNotFoundException, InterruptedException {

        // create MR Job to load edges into Titan from configuration and
        // initialize MR parameters

        Job writeEdgesJob =
                new Job(conf,
                        "TitanWriterMRChain Job 2: Writing Edges to Titan");
        writeEdgesJob.setJarByClass(TitanWriterMRChain.class);

        // configure mapper  and input

        writeEdgesJob.setMapperClass(PassThruMapperIntegerKey.class);

        writeEdgesJob.setMapOutputKeyClass(IntWritable.class);
        writeEdgesJob.setMapOutputValueClass(mapValueType.getClass());

        // we read from the temporary storage location...

        writeEdgesJob.setInputFormatClass(SequenceFileInputFormat.class);

        try {
            FileInputFormat.addInputPath(writeEdgesJob,
                    intermediateDataFilePath);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode
                    .UNHANDLED_IO_EXCEPTION,
                    "GRAPHBUILDER_ERROR: Cannot access temporary edge file.",
                    LOG, e);
        }

        // configure reducer

        writeEdgesJob.setReducerClass(IntermediateEdgeWriterReducer.class);

        // now we set up those temporary storage locations...;

        writeEdgesJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        writeEdgesJob.setOutputKeyClass(IntWritable.class);
        writeEdgesJob.setOutputValueClass(mapValueType.getClass());

        FileOutputFormat.setOutputPath(writeEdgesJob,
                intermediateEdgeFilePath);

        LOG.info("=========== Job 2: Propagate Titan Vertex IDs to Edges " +
                "  ===========");

        LOG.info("==================== Start " +
                "====================================");
        writeEdgesJob.waitForCompletion(true);
        LOG.info("=================== Done " +
                "====================================\n");
    }

    private void runEdgeLoadMapOnlyJob(Path intermediateEdgeFilePath)
            throws IOException, ClassNotFoundException, InterruptedException {

        // create MR Job to load edges into Titan from configuration and
        // initialize MR parameters

        Job addEdgesJob = new Job(conf, "TitanWriterMRChain Job 3: " +
                "Writing " +
                "Edges to Titan");
        addEdgesJob.setJarByClass(TitanWriterMRChain.class);

        // configure mapper  and input

        addEdgesJob.setMapperClass(EdgesIntoTitanMapper.class);

        addEdgesJob.setMapOutputKeyClass(NullWritable.class);
        addEdgesJob.setMapOutputValueClass(NullWritable.class);

        // configure reducer

        addEdgesJob.setNumReduceTasks(0);

        // we read from the temporary storage location...

        addEdgesJob.setInputFormatClass(SequenceFileInputFormat.class);

        try {
            FileInputFormat.addInputPath(addEdgesJob,
                    intermediateEdgeFilePath);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode
                    .UNHANDLED_IO_EXCEPTION,
                    "GRAPHBUILDER_ERROR: Cannot access temporary edge file.",
                    LOG, e);
        }

        addEdgesJob.setOutputFormatClass(org.apache.hadoop.mapreduce.lib
                .output.NullOutputFormat.class);

        LOG.info("=========== Job 3: Add edges to Titan " +
                "  ===========");

        LOG.info("==================== Start " +
                "====================================");
        addEdgesJob.waitForCompletion(true);
        LOG.info("=================== Done " +
                "====================================\n");
    }
}   // End of TitanWriterMRChain

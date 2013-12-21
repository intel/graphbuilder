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

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphTokenizer;
import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * This class implements the basic mapper {@code context.writes} tasks 
 * of the keyed property graph elements.
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.input.hbase.HBaseReaderMapper
 */
public class BaseMapper {

    /**
     * Implements the vertex and edge counters. You should never call this 
	 * directly, use the getter methods instead.
     */
    private enum Counters {
        VERTEX_WRITE_ERROR,
        EDGE_WRITE_ERROR,
    }

    private Logger               log;
    private IntWritable          mapKey;
    private SerializedGraphElement mapVal;
    private Class                valClass;
    private GraphTokenizer       tokenizer;
    private KeyFunction          keyFunction;
    private Mapper.Context       context;
    private Configuration        conf;

    /**
     * An Exception construction will log a fatal error and cause a {@code system.exit}. There is no point in 
     * going forward if we can't initialize the tokenizer class, key function, map val, or map key.
     *
     * @param {@code context}  The {@code Mapper.Context} for the running mapper.
     * @param {@code conf}     The current conf for the mapper.
     * @param {@code log}      The log instance so all logs are attributed to the calling class.
     */
    public BaseMapper(Mapper.Context context, Configuration conf, Logger log) {
        this.context = context;
        this.log     = log;
        this.conf    = conf;
        setUp(conf);
    }

    /**
     * Mapper bootstrapping. Initializes the tokenizer for parsing the edges and vertices, the key 
     * function for getting the {@code context.write} key, and initializes the {@code mapKey mapValue}. 
     * The {@code InstantiationException}, {@code IllegalAccessException}, and {@code ClassNotFoundException} will all be caught,  
     * logged, and a system exit will be called. There is no reason to continue if we can't boot strap.
     *
     * @param {@code conf}  The mapper's current configuration, usually {@code context.getConfiguration()}.
     */
    public void setUp(Configuration conf) {

        initializeTokenizer(conf);
        initializeKeyFunction(conf);
        setValClass(context.getMapOutputValueClass());

        try {
            setMapVal((SerializedGraphElement) valClass.newInstance());
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Cannot instantiate map value class (" + SerializedGraphElement.class.getName() + " )", log, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Illegal access exception when instantiating map value class ("
                            + SerializedGraphElement.class.getName() + " )", log, e);
        }

        setMapKey(new IntWritable());
    }

    /**
     * A wrapper method to initialize the key function. This makes it easier to mock-up in a unit test,
	 * and it is a general good practice to encapsulate.
     *
     * @param {@code conf}  The mappers configuration, usually {@code context.getConfiguration()}.
     */

    protected void initializeKeyFunction(Configuration conf) {
        try {
            this.keyFunction = (KeyFunction) Class.forName(conf.get("KeyFunction")).newInstance();
        } catch (ClassNotFoundException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Could not find class named for key function.", log, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Illegal access exception when instantiating key function.", log, e);
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Instantiation exception when instantiating key function.", log, e);
        }
    }

    /**
     * A wrapper method that initializes the tokenizer. This makes it easier to mockup in a unit test, 
     * and it is a general good practice to encapsulate.
     *
     * @param {@code conf}  The mappers configuration, usually {@code context.getConfiguration()}.
     */
    protected void initializeTokenizer(Configuration conf) {
        try {
            this.tokenizer = (GraphTokenizer) Class.forName(conf.get("GraphTokenizer")).newInstance();
        } catch (ClassNotFoundException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Could not find class named for tokenizer.", log, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Illegal access exception when instantiating tokenizer.", log, e);
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "Instantiation exception when instantiating tokenizer.", log, e);
        }
        this.tokenizer.configure(conf);
    }

    /**
     * Increments the correct error counter, either the Vertex or Edge error counter.
     *
     * @param {@code context}  The current context for the mapper.
     * @param {@code val}      The {@code SerializedGraphElement} that threw the error.
     */
    protected void incrementErrorCounter(Mapper.Context context, SerializedGraphElement val) {
        if (val.graphElement().isEdge()) {
            context.getCounter(getEdgeWriteErrorCounter()).increment(1);
        } else if (val.graphElement().isVertex()) {
            context.getCounter(getVertexWriteErrorCounter()).increment(1);
        }
    }

    /**
     * Attempts to write the key and value pair. IOException or InterruptedException will be logged and 
     * the appropriate edge or vertex counter will be incremented.
     *
     * @param {@code context}  The current mapper context.
     * @param {@code key}      The vertex and edge key  to write.
     * @param {@code val}      The property graph element to write, either vertex or edge.
     */
    protected void contextWrite(Mapper.Context context, IntWritable key, SerializedGraphElement val) {
        try {
            context.write(key, val);
        } catch (IOException e) {
            incrementErrorCounter(context, val);
            log.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            incrementErrorCounter(context, val);
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Iterates through the edge list, creates the Edge graph element, gets its key and writes it. 
     * {@code NullPointerException}s are captured whenever edge or vertex has any null values.
     *
     * @param {@code context}  The mapper's current context.
     */
    public void writeEdges(Mapper.Context context) {
        try {
             Iterator<Edge> edgeIterator = tokenizer.getEdges();

            while (edgeIterator.hasNext()) {

                Edge edge = edgeIterator.next();

                mapVal.init(edge);
                mapKey.set(keyFunction.getEdgeKey(edge));

                contextWrite(context, mapKey, mapVal);
            }
        } catch (NullPointerException e) {
            context.getCounter(getEdgeWriteErrorCounter()).increment(1);
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Iterates through the vertex list, creates a vertex graph element, gets its key, and writes it. 
     * {@code NullPointerException}s are captured whenever the edge or vertex has any null values.
     *
     * @param {@code context} The mapper's current context.
     */
    public void writeVertices(Mapper.Context context) {
        try {
            Iterator<Vertex> vertexIterator = tokenizer.getVertices();
            while (vertexIterator.hasNext()) {

                Vertex vertex = vertexIterator.next();

                mapVal.init(vertex);
                mapKey.set(keyFunction.getVertexKey(vertex));

                contextWrite(context, mapKey, mapVal);
            }
        } catch (NullPointerException e) {
            context.getCounter(getVertexWriteErrorCounter()).increment(1);
            log.error(e.getMessage(), e);
        }
    }

    public void setMapKey(IntWritable mapKey) {
        this.mapKey = mapKey;
    }

    public void setMapVal(SerializedGraphElement mapVal) {
        this.mapVal = mapVal;
    }

    public void setValClass(Class valClass) {
        this.valClass = valClass;
    }

    public GraphTokenizer getTokenizer() {
        return tokenizer;
    }

    /**
     * Gets the edge write error counter. This will make it easier to change the enum in the future if 
     * we need to, without affecting other code.
     *
     * @return Counter
     */
    public static Counters getEdgeWriteErrorCounter() {
        return Counters.EDGE_WRITE_ERROR;
    }

    /**
     * Gets the vertex write error counter. This will make it easier to change the enum in the future if 
     * we need to, without affecting other code.
     *
     * @return Counter
     */
    public static Counters getVertexWriteErrorCounter() {
        return Counters.VERTEX_WRITE_ERROR;
    }
}

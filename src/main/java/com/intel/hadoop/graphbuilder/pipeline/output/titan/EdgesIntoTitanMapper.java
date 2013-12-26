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

import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.output.GraphElementWriter;
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.GraphDatabaseConnector;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * This class reads edges from HDFS by IntermediateEdgeWriterReducer. The
 * Titan ID of both source and target vertices are written along with
 * the edge properties in the previous step. In this map-only job,
 * the map function fetches the reference to the source and target vertices
 * from Titan and adds the edges to Titan using the Blueprints addEdge() API.
 * The edge properties are also written to Titan
 */

public class EdgesIntoTitanMapper extends Mapper<IntWritable,
        SerializedGraphElement, NullWritable, NullWritable> {
    private static final Logger LOG = Logger.getLogger(
            EdgesIntoTitanMapper.class);

    private static enum Counters {
        EDGE_PROPERTIES_WRITTEN,
        NUM_EDGES
    }

    TitanGraph graph;

    private TitanGraph getTitanGraphInstance (Context context) throws
            IOException {
        BaseConfiguration titanConfig = new BaseConfiguration();
        return GraphDatabaseConnector.open("titan",
                titanConfig,
                context.getConfiguration());
    }

    /**
     * Sets up the Titan connection.
     *
     * @param {@code context}  The reducer context provided by Hadoop.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {

        this.graph = getTitanGraphInstance(context);
    }

    /**
     * The map function reads the references of the source and target
     * vertices of a given edge and writes the edge and its properties to
     * Titan using the Blueprints addEdge() call
     *
     * @param key The data structure of the input file is every record
     *            per line is a serialized edge. Input of this
     *            map comes from the output directory written by
     *            IntermediateEdgeWriterReducer.java
     * @param value A serialized edge
     * @param context Hadoop Job context
     */

    @Override
    public void map(IntWritable key, SerializedGraphElement value,
                    Context context) throws IOException, InterruptedException {

        SerializedGraphElement serializedGraphElement = value;

        if (serializedGraphElement.graphElement().isVertex()) {
            // This is a strange case, throw an exception
            throw new IllegalArgumentException("GRAPHBUILDER_ERROR: " +
                    "Found unexpected Vertex element in the edge write " +
                    "mapper. Please recheck the logic to create vertices and " +
                    "edges.");
        }

        com.tinkerpop.blueprints.Vertex srcBlueprintsVertex =
                    this.graph.getVertex(serializedGraphElement.graphElement
                            ().getProperty(GraphElementWriter
                            .PROPERTY_KEY_SRC_TITAN_ID));
        com.tinkerpop.blueprints.Vertex tgtBlueprintsVertex =
                    this.graph.getVertex(serializedGraphElement.graphElement
                            ().getProperty(GraphElementWriter
                            .PROPERTY_KEY_TGT_TITAN_ID));
        PropertyMap propertyMap = (PropertyMap) serializedGraphElement
                .graphElement().getProperties();

        // Add the edge to Titan graph

        com.tinkerpop.blueprints.Edge bluePrintsEdge = null;
        String edgeLabel = serializedGraphElement.graphElement().getLabel()
                .toString();
        try {

            bluePrintsEdge = this.graph.addEdge(null,
                        srcBlueprintsVertex,
                        tgtBlueprintsVertex,
                        edgeLabel);

        } catch (IllegalArgumentException e) {

            GraphBuilderExit.graphbuilderFatalExitException(
                        StatusCode.TITAN_ERROR,
                        "Could not add edge to Titan; likely a schema error. " +
                        "The label on the edge is  " + edgeLabel, LOG, e);
        }

        // The edge is added to the graph; now add the edge properties.

        // The "srcTitanID" property was added during this MR job to
        // propagate the Titan ID of the edge's source vertex to this
        // reducer ... we can remove it now.

        propertyMap.removeProperty(GraphElementWriter
                .PROPERTY_KEY_SRC_TITAN_ID);
        propertyMap.removeProperty(GraphElementWriter
                .PROPERTY_KEY_TGT_TITAN_ID);

        for (Writable propertyKey : propertyMap.getPropertyKeys()) {
           EncapsulatedObject mapEntry = (EncapsulatedObject)
                        propertyMap.getProperty(propertyKey.toString());

           try {
               bluePrintsEdge.setProperty(propertyKey.toString(),
                       mapEntry.getBaseObject());
           } catch (IllegalArgumentException e) {
               LOG.fatal("GRAPHBUILDER_ERROR: Could not add edge " +
                            "property; probably a schema error. The label on " +
                            "the edge is  " + edgeLabel);
               LOG.fatal("GRAPHBUILDER_ERROR: The property on the edge " +
                            "is " + propertyKey.toString());
               LOG.fatal(e.getMessage());
               GraphBuilderExit.graphbuilderFatalExitException
                            (StatusCode.INDESCRIBABLE_FAILURE, "", LOG, e);
           }
        }

        context.getCounter(Counters.NUM_EDGES).increment(1L);
    }   // End of map function

    /**
     * Performs cleanup tasks after the reducer finishes.
     *
     * In particular, closes the Titan graph.
     * @param {@code context}  Hadoop provided reducer context.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {
        this.graph.shutdown();
    }

    public  Enum getEdgeCounter(){
        return Counters.NUM_EDGES;
    }

    public Enum getEdgePropertiesCounter(){
        return Counters.EDGE_PROPERTIES_WRITTEN;
    }
}

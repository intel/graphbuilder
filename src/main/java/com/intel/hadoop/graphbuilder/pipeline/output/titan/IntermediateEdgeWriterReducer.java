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

import com.intel.hadoop.graphbuilder.graphelements.*;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.SourceVertexKeyFunction;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.GraphDatabaseConnector;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

/**
 * Write edges with Titan vertex ID of the target vertex to HDFS.
 * <p>
 * This class gathers each vertex with the edges that point to that vertex,
 * that is, those edges for which the vertex is the destination. Because the
 * edges were tagged with the Titan IDs of their sources in the previous Map
 * Reduce job and each vertex is tagged with its Titan ID,
 * we now know the Titan ID of the source and destination of the edges and
 * can add them to Titan.
 * </p>
 */

public class IntermediateEdgeWriterReducer extends Reducer<IntWritable,
        SerializedGraphElement, IntWritable, SerializedGraphElement> {
    private static final Logger LOG = Logger.getLogger(
                IntermediateEdgeWriterReducer.class);

    private Hashtable<Object, Long> vertexNameToTitanID;

    private IntermediateEdgeWriterReducerCallback intermediateEdgeWriterReducerCallback;

    private final KeyFunction keyFunction = new SourceVertexKeyFunction();
    private IntWritable            outKey;
    private SerializedGraphElement outValue;
    private Class                  outClass;

    private static enum Counters {
        EDGE_PROPERTIES_WRITTEN,
        NUM_EDGES
    }

    /*
     * Creates the Titan graph for saving edges and removes the static open
     * method from setup so it can be mocked-up.
     *
     * @return {@code TitanGraph}  For saving edges.
     * @throws IOException
     */
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

        this.vertexNameToTitanID = new Hashtable<Object, Long>();

        intermediateEdgeWriterReducerCallback = new IntermediateEdgeWriterReducerCallback();

        outClass = context.getMapOutputValueClass();
        outKey   = new IntWritable();

        try {
            outValue   = (SerializedGraphElement) outClass.newInstance();
        } catch (InstantiationException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode
                    .CLASS_INSTANTIATION_ERROR, "GRAPHBUILDER_ERROR: Cannot " +
                    "instantiate new reducer output value ( " + outClass
                    .getName() + ")", LOG, e);
        } catch (IllegalAccessException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode
                    .CLASS_INSTANTIATION_ERROR, "GRAPHBUILDER_ERROR: Illegal " +
                    "access exception when instantiating reducer output value" +
                    " ( " + outClass.getName() + ")", LOG, e);
        }

    }

    /**
     * This routine is called by Hadoop to write edges to an
     * intermediate HDFS  location
     * <p>
     * We assume that the edges and vertices have been gathered so that every
     * edge shares the reducer of its destination vertex,
     * and that every edge has previously been assigned the Titan ID of its
     * source vertex.
     * </p>
     * <p>
     * Titan IDs are propagated from the destination vertices to each edge
     * </p>
     * @param {@code key}      A map reduce key; a hash of a vertex ID.
     * @param {@code values}   Either a vertex with that hashed vertex ID,
     *                         or an edge with said vertex as its destination.
     * @param {@code context}  A reducer context provided by Hadoop.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(IntWritable key, Iterable<SerializedGraphElement>
            values, Context context)
            throws IOException, InterruptedException {

        Hashtable<EdgeID, Writable> edgePropertyTable  = new Hashtable();

        for(SerializedGraphElement graphElement: values){
            /*
             * This is calling IntermediateEdgeWriterReducerCallback which is an
             * implementation of GraphElementTypeCallback to add all the
             * edges and vertices into  the edgePropertyTable and
             * vertexNameToTitanID hashmaps
             */
            graphElement.graphElement().typeCallback(
                    intermediateEdgeWriterReducerCallback,
                    ArgumentBuilder.newArguments()
                            .with("edgePropertyTable", edgePropertyTable)
                            .with("vertexNameToTitanID",
                    vertexNameToTitanID));
        }

        int edgeCount   = 0;
        StringType edgeLabel = new StringType();

        // Output edge records

        for (Map.Entry<EdgeID, Writable> edgeMapEntry :
                    edgePropertyTable.entrySet()) {

            VertexID<StringType> srcVertexId = (VertexID<StringType>)
                    edgeMapEntry.getKey().getSrc();
            VertexID<StringType> tgtVertexId = (VertexID<StringType>)
                    edgeMapEntry.getKey().getDst();
            edgeLabel.set(edgeMapEntry.getKey().getLabel().toString());
            PropertyMap propertyMap = (PropertyMap) edgeMapEntry.getValue();

            Edge tempEdge = new Edge();
            tempEdge.configure(srcVertexId, tgtVertexId, edgeLabel, propertyMap);

            // Add the Titan ID of the target vertex

            long dstTitanId
                    = vertexNameToTitanID.get(edgeMapEntry.getKey().getDst());
            tempEdge.setProperty("tgtTitanID", new LongType(dstTitanId));

            outValue.init(tempEdge);
            outKey.set(keyFunction.getEdgeKey(tempEdge));

            context.write(outKey, outValue);

            tempEdge = null;

            edgeCount++;

         }  // End of for loop on edges

        context.getCounter(Counters.NUM_EDGES).increment(edgeCount);
    }   // End of reduce

    public  Enum getEdgeCounter(){
        return Counters.NUM_EDGES;
    }

    public Enum getEdgePropertiesCounter(){
        return Counters.EDGE_PROPERTIES_WRITTEN;
    }
}

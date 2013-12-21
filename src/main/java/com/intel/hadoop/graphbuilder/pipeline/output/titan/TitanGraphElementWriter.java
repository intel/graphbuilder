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

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.graphelements.VertexID;
import com.intel.hadoop.graphbuilder.pipeline.output.GraphElementWriter;
import com.intel.hadoop.graphbuilder.types.EncapsulatedObject;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import com.thinkaurelius.titan.core.TitanElement;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

/**
 * - duplicate edges and vertices are removed
 * - each vertex is loaded into Titan and is tagged with its Titan ID and passed to the next MR job
 *   through the temp file
 * - each edge is tagged with the Titan ID of its source vertex and passed to the next MR job
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.mergeduplicates.GraphElementMerge
 */
public class TitanGraphElementWriter extends GraphElementWriter {
    private Hashtable<Object, Long>  vertexNameToTitanID = new Hashtable<>();

    /**
     * Write graph elements to a Titan graph instance.
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(ArgumentBuilder args)
            throws IOException, InterruptedException {
        initArgs(args);

        writeVertices(args);

        writeEdges(args);
    }

    /**
     * Obtain the Titan-assigned ID from a Blueprints vertex
     * @param bpVertex  A Blueprints vertex.
     * @return Its Titan-assigned ID.
     */

    public long getVertexId(com.tinkerpop.blueprints.Vertex bpVertex){
        return ((TitanElement)bpVertex).getID();
    }

    /**
     * Writes vertices to a Titan graph and propagate its Titan-ID through an HDFs file.
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void writeVertices(ArgumentBuilder args) throws IOException, InterruptedException {
        initArgs(args);

        int vertexCount = 0;

        for(Map.Entry<Object, Writable> vertex: vertexSet.entrySet()) {
            // Major operation - vertex is added to Titan and a new ID is assigned to it
            com.tinkerpop.blueprints.Vertex  bpVertex = graph.addVertex(null);

            bpVertex.setProperty(TitanConfig.GB_ID_FOR_TITAN, vertex.getKey().toString());

            long vertexId = getVertexId(bpVertex);

            Vertex tempVertex = new Vertex();

            tempVertex.configure((VertexID) vertex.getKey(), writeVertexProperties(vertexId, vertex, bpVertex));

            outValue.init(tempVertex);
            outKey.set(keyFunction.getVertexKey(tempVertex));

            context.write(outKey, outValue);

            vertexNameToTitanID.put(vertex.getKey(), vertexId);

            vertexCount++;
        }

        context.getCounter(vertexCounter).increment(vertexCount);
    }

    /**
     * Append the Titan ID of an edge's source to the edge as a property, and write the edge to an HDFS file.
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void writeEdges(ArgumentBuilder args)
            throws IOException, InterruptedException {
        initArgs(args);

        int edgeCount   = 0;

        for(Map.Entry<EdgeID, Writable> edge: edgeSet.entrySet()){
            Object src                  = edge.getKey().getSrc();
            Object dst                  = edge.getKey().getDst();
            String label                = edge.getKey().getLabel().toString();

            long srcTitanId = vertexNameToTitanID.get(src);

            Edge tempEdge = new Edge();

            writeEdgeProperties(srcTitanId, edge);

            tempEdge.configure((VertexID)  src, (VertexID)  dst, new StringType(label),
                    writeEdgeProperties(srcTitanId, edge));

            outValue.init(tempEdge);
            outKey.set(keyFunction.getEdgeKey(tempEdge));

            context.write(outKey, outValue);

            edgeCount++;
        }
        context.getCounter(edgeCounter).increment(edgeCount);

    }

    private PropertyMap writeVertexProperties(long vertexId, Map.Entry<Object, Writable> vertex, com.tinkerpop.blueprints.Vertex bpVertex){

        PropertyMap propertyMap = (PropertyMap) vertex.getValue();

        if(propertyMap == null){
            propertyMap = new PropertyMap();
        }

        for (Writable keyName : propertyMap.getPropertyKeys()) {
            EncapsulatedObject mapEntry = (EncapsulatedObject) propertyMap.getProperty(keyName.toString());

            bpVertex.setProperty(keyName.toString(), mapEntry.getBaseObject());
        }

        addTitanIdVertex(vertexId, propertyMap);

        return propertyMap;
    }

    private PropertyMap addTitanIdVertex(long vertexId, PropertyMap propertyMap){
        propertyMap.setProperty("TitanID", new LongType(vertexId));
        return propertyMap;
    }

    private PropertyMap addTitanIdEdge(long srcTitanId, PropertyMap propertyMap){
        propertyMap.setProperty("srcTitanID", new LongType(srcTitanId));
        return propertyMap;
    }

    private PropertyMap writeEdgeProperties(long srcTitanId, Map.Entry<EdgeID, Writable> edge){

        PropertyMap propertyMap = (PropertyMap) edge.getValue();

        if(propertyMap == null){
            propertyMap = new PropertyMap();
        }

        addTitanIdEdge(srcTitanId, propertyMap);

        return propertyMap;
    }
}

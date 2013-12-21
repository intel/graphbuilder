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
import com.intel.hadoop.graphbuilder.graphelements.callbacks.GraphElementTypeCallback;
import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import org.apache.hadoop.io.Writable;

import java.util.Hashtable;

/**
 *  Adds all of the edges and vertices into the their respective hashmaps. The 
 *  {@code edgePropertyTable} hash is keyed by edge id (src, dst, label) with 
 *  the value being the property map. The {@code vertexNameToTitanID} is keyed 
 *  by vertex id, which should be a {@code StringType} or {@code LongType} 
 *  with the value being the titan ID for the vertex.
 *
 * @see LongType
 * @see com.intel.hadoop.graphbuilder.types.StringType
 * @see EdgesIntoTitanReducer
 */
public class EdgesIntoTitanReducerCallback implements GraphElementTypeCallback {
    private Hashtable<EdgeID, Writable> edgePropertyTable;
    private Hashtable<Object, Long> vertexNameToTitanID;

    @Override
    public Hashtable<EdgeID, Writable> edge(GraphElement graphElement, ArgumentBuilder  args) {
        initArguments(args);

        Edge edge   = (Edge) graphElement;
        EdgeID edgeID = new EdgeID(edge.getSrc(), edge.getDst(), edge.getLabel());

        edgePropertyTable.put(edgeID, edge.getProperties());

        return edgePropertyTable;
    }

    @Override
    public Hashtable<Object, Long> vertex(GraphElement graphElement, ArgumentBuilder args) {
        initArguments(args);

        Vertex vertex = (Vertex) graphElement;

        VertexID vertexId      = vertex.getId();
        PropertyMap propertyMap   = vertex.getProperties();
        //the Titan id we got assigned during the VerticesIntoTitanReducer
        long        vertexTitanId = ((LongType) propertyMap.getProperty("TitanID")).get();

        vertexNameToTitanID.put(vertexId, vertexTitanId);
        return vertexNameToTitanID;
    }

    private void initArguments(ArgumentBuilder args){
        if(args.exists("edgePropertyTable") && args.exists("vertexNameToTitanID")){
            edgePropertyTable = (Hashtable<EdgeID, Writable>)args.get("edgePropertyTable");
            vertexNameToTitanID = (Hashtable<Object, Long>)args.get("vertexNameToTitanID");
        }else{
            throw new IllegalArgumentException("edgePropertyTable and vertexNameToTitanID were not set" );
        }
    }
}

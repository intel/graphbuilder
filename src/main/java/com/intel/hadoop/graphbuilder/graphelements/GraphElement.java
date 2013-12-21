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
package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.graphelements.callbacks.*;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Base for the vertex or edge graph element.
 * @param {@code <VidType>}  The vertex ID type.
 * @code
 */
public abstract class GraphElement<VidType extends WritableComparable<VidType>> {

    public abstract boolean isEdge();
    public abstract boolean isVertex();
    public abstract boolean isNull();
    public abstract String toString();
    public abstract void write(DataOutput output) throws IOException;
    public abstract PropertyMap getProperties();
    //get a property from the property map
    public abstract Object getProperty(String key);
    //get the vertex or edge label
    public abstract StringType getLabel();
    //get the vertex id or EdgeID
    public abstract Object getId();
    //get the graph element
    public abstract GraphElement get();

    /**
     * All of the callback classes we will be using.
     */
    private final GraphElementType graphElementType;
    private final GraphElementDst graphElementDst;
    private final GraphElementSrc graphElementSrc;

    public GraphElement(){
        graphElementType = new GraphElementType();
        graphElementDst  = new GraphElementDst();
        graphElementSrc  = new GraphElementSrc();
    }

    /**
     * Calls the edge or vertex {@code GraphElementTypeCallback}.
     *
     * @see com.intel.hadoop.graphbuilder.graphelements.callbacks.GraphElementTypeCallback
     *
     * @param {@code graphElementTypeCallback}  Any instance of 
	 *                                         {@code GraphElementTypeCallback}.
     * @param {@code args}                     The variable length of arguments that 
	 *                                         might be used by the instance of 
	 *                                         {@code GraphElementTypeCallback}.
     * @return anything that gets returned by the instance of GraphElementTypeCallback
     */
    public  <T> T typeCallback(GraphElementTypeCallback graphElementTypeCallback, ArgumentBuilder args){
        if(this.isEdge()){
            return graphElementTypeCallback.edge(this, args);
        }else if(this.isVertex()){
            return graphElementTypeCallback.vertex(this, args);
        }
        return null;
    }

    public  <T> T typeCallback(GraphElementTypeCallback graphElementTypeCallback){
        ArgumentBuilder args = ArgumentBuilder.newArguments();

        if(this.isEdge()){
            return graphElementTypeCallback.edge(this, args);
        }else if(this.isVertex()){
            return graphElementTypeCallback.vertex(this, args);
        }
        return null;
    }

    public Enum getType(){
        return this.typeCallback(graphElementType);
    }

    public Object getDst(){
        return this.typeCallback(graphElementDst);
    }

    public Object getSrc(){
        return this.typeCallback(graphElementSrc);
    }
}



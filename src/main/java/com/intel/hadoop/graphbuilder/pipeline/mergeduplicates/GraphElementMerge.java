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
package com.intel.hadoop.graphbuilder.pipeline.mergeduplicates;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import com.intel.hadoop.graphbuilder.graphelements.callbacks.GraphElementTypeCallback;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import com.intel.hadoop.graphbuilder.util.Functional;
import org.apache.hadoop.io.Writable;

import java.util.Hashtable;

/**
 * <p>
 * Remove any duplicate edges and vertices. If duplicates are found, either merge 
 * their property maps or call an optional Edge/Vertex reducer.
 * </p>
 * <p>
 * This will be called on the property graph element as we are iterating through 
 * the list received by the reducer.
 * </p>
 *
 * <p>
 * All arguments are extracted from the argument builder and are all mandatory, 
 * except the {@code edgeReducerFunction} and the {@code vertexReducerFunction}.
 * <ul>
 *      <li>{@code edgeSet}                Hashtable with the current list of merged edges.</li>
 *      <li>{@code vertexSet}              Hashtable with the current list of merged vertices.</li>
 *      <li>{@code edgeReducerFunction}    Optional edge reducer function.</li>
 *      <li>{@code vertexReducerFunction}  Optional vertex reducer function.</li>
 *      <li>{@code vertexLabelMap}         List of vertex labels to be used for writing rdf output.</li>
 *      <li>{@code noBiDir}                Are we cleaning bidirectional edges. If true then remove bidirectional edge.</li>
 * </ul>
 * </p>
 */
public class GraphElementMerge implements GraphElementTypeCallback {

    private Hashtable<EdgeID, Writable> edgeSet;
    private Hashtable<Object, Writable>   vertexSet;
    private Hashtable<Object, StringType>    vertexLabelMap;

    private Functional edgeReducerFunction;
    private Functional vertexReducerFunction;

    private boolean noBiDir = false;

    /**
     *
     * @param graphElement the property graph element we will check for duplicates
     * @param args list of arguments
     * @return the updated edge set
     */
    @Override
    public Hashtable<EdgeID, Writable> edge(GraphElement graphElement, ArgumentBuilder args) {
        this.arguments(args);

        EdgeID edgeID = (EdgeID) graphElement.getId();

        if (((Edge) graphElement).isSelfEdge()) {
            // Self edges are omitted.
            return null;
        }

        if(edgeSet.containsKey(graphElement.getId())){
            // Edge is a duplicate.

            if (edgeReducerFunction != null) {
                edgeSet.put(edgeID, edgeReducerFunction.reduce(graphElement.getProperties(), edgeSet.get(edgeID)));
            } else {

                /**
                 * The default behavior is to merge the property maps of duplicate edges.
                 * Any conflicting key/value pairs get overwritten.
                 */

                PropertyMap existingPropertyMap = (PropertyMap) edgeSet.get(edgeID);
                existingPropertyMap.mergeProperties(graphElement.getProperties());
            }

        }else{
            if (noBiDir && edgeSet.containsKey(edgeID.reverseEdge())) {
                // In this case, skip the bi-directional edge.
            } else {
                // This edge is either not bi-directional, or we are keeping bi-directional edges.
                if (edgeReducerFunction != null) {
                    edgeSet.put(edgeID, edgeReducerFunction.reduce(graphElement.getProperties(),edgeReducerFunction.identityValue()));
                } else {
                    edgeSet.put(edgeID, graphElement.getProperties());
                }
            }
        }
        return edgeSet;
    }

    /**
     *
     * @param graphElement the property graph element we will check for duplicates
     * @param args see the arguments method for the expected argument list
     * @return updated vertex set
     */
    @Override
    public Hashtable<Object, Writable>  vertex(GraphElement graphElement, ArgumentBuilder args) {
        this.arguments(args);

        Object vid = graphElement.getId();

        // track the RDF labels of vertices
        if (graphElement.getLabel() != null && vertexLabelMap != null) {
            if (!vertexLabelMap.containsKey(graphElement.getId())) {
                vertexLabelMap.put(graphElement.getId(), (StringType) graphElement.getLabel());
            }
        }

        if(vertexSet.containsKey(graphElement.getId())){
            if (vertexReducerFunction != null) {
                vertexSet.put(vid,
                        vertexReducerFunction.reduce(graphElement.getProperties(),
                                vertexSet.get(vid)));
            } else {

                /**
                 * The default behavior is to merge the property maps of duplicate vertices.
                 * Any conflicting key/value pairs get overwritten.
                 */

                PropertyMap existingPropertyMap = (PropertyMap) vertexSet.get(vid);
                existingPropertyMap.mergeProperties(graphElement.getProperties());
            }

        }else{
            if (vertexReducerFunction != null) {
                vertexSet.put(vid, vertexReducerFunction.reduce(
                        graphElement.getProperties(),vertexReducerFunction.identityValue()));
            } else {
                vertexSet.put(vid, graphElement.getProperties());
            }
        }
        return vertexSet;
    }

    /**
     * Gets all of our arguments from the argument builder.
     * <ul>
     *      <li>{@code edgeSet}                Hashtable with the current list of merged edges.</li>
     *      <li>{@code vertexSet}              Hashtable with the current list of merged vertices.</li>
     *      <li>{@code edgeReducerFunction}    Optional edge reducer function.</li>
     *      <li>{@code vertexReducerFunction}  Optional vertex reducer function</li>
     *      <li>{@code vertexLabelMap}         A list of vertex labels to be used for writing rdf output</li>
     *      <li>{@code noBiDir}                Are we cleaning bidirectional edges. If true then remove 
	 *                                         bidirectional edge.</li>
     * </ul>
     * @param {@code args}  An {@code ArgumentBuilder} with all the necessary arguments.
     *
     * @see Functional
     */
    private void arguments(ArgumentBuilder args){
        edgeSet               = (Hashtable<EdgeID, Writable>)args.get("edgeSet");
        vertexSet             = (Hashtable<Object, Writable>)args.get("vertexSet");
        edgeReducerFunction   = (Functional)args.get("edgeReducerFunction", null);
        vertexReducerFunction = (Functional)args.get("vertexReducerFunction", null);
        vertexLabelMap        = (Hashtable<Object, StringType>)args.get("vertexLabelMap", null);
        noBiDir               = (boolean)args.get("noBiDir");
    }
}

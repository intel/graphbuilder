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
package com.intel.hadoop.graphbuilder.graphelements.callbacks;

import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;

/**
 * Returns the type of the graph element. Very useful when you just want a type,
 * and you don't care if it's an edge or vertex.
 *
 * @see com.intel.hadoop.graphbuilder.graphelements.GraphElement
 */
public class GraphElementType implements GraphElementTypeCallback {
    public enum GraphType {EDGE, VERTEX}

    /**
     * Returns the edge's enum type. Although the {@code graphElement} and arguments 
	 * are not used here, they must be part of the method definition to satisfy 
	 * the implementation.
     *
     * @param {@code graphElement}  The graph element to perform operations on.
     * @param {@code arguments}     Any arguments that might have been passed.
     * @return  The edge's enum type.
     */
    @Override
    public GraphType edge(GraphElement graphElement, ArgumentBuilder arguments) {
        return GraphType.EDGE;
    }

    /**
     * Returns the vertex enum type. Although the {@code graphElement} and 
	 * {@code arguments} are not used here, they must be part of the
     * method definition to satisfy the implementation.
     *
     * @param {@code graphElement}  The graph element to perform operations on.
     * @param {@code arguments}     Any arguments that might have been passed.
     * @return The vertex's enum type.
     */
    @Override
    public GraphType vertex(GraphElement graphElement, ArgumentBuilder arguments) {
        return GraphType.VERTEX;
    }
}

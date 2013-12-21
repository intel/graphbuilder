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

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;

/**
 * Gets the graph elements {@code dst}. If it's an edge it will return 
 * the {@code dst}, otherwise it will return null.
 * @see com.intel.hadoop.graphbuilder.graphelements.GraphElement
 */
public class GraphElementDst implements GraphElementTypeCallback {

    /**
     * Casts our graph element to an edge and get the edge's {@code dst}.
     *
     * @param {@code graphElement}  The graph element to perform operations on.
     * @param {@code arguments}     Any arguments that might have been passed
     * @return The edge's {@code dst}.
     */
    @Override
    public Object edge(GraphElement graphElement, ArgumentBuilder arguments) {
        Edge edge = (Edge) graphElement;
        return edge.getDst();
    }

    /**
     * Returns null because vertices do not have a {@code dst}.
     *
     * @param {@code graphElement} The graph element to perform operations on.
     * @param {@code arguments}    Any arguments that might have been passed.
     * @return Always null because vertices do not have a {@code dst}.
     */
    @Override
    public Object vertex(GraphElement graphElement, ArgumentBuilder arguments) {
        return null;
    }
}

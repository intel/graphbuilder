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
package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.StringType;

public class DestinationVertexKeyFunctionTest {

    @Test
    public void testGetEdgeKey() throws Exception {

        DestinationVertexKeyFunction keyFunction = new DestinationVertexKeyFunction();

        StringType sourceId1 = new StringType("Scooby Doo Meets ");
        StringType sourceId2 = new StringType("The Further Adventures of ");
        StringType destId    = new StringType(" Batman and Robin");

        StringType label  = new StringType("Cartoon");

        Edge<StringType> edge1 = new Edge<StringType>(sourceId1, destId, label);
        Edge<StringType> edge2 = new Edge<StringType>(sourceId2, destId, new StringType("comic book"));

        assertEquals(keyFunction.getEdgeKey(edge1), keyFunction.getEdgeKey(edge2));

        StringType altDstId = new StringType(" Don Amece");
        Edge<StringType> oddEdgeOut = new Edge<StringType>(sourceId1, altDstId, label);

        // technically, this could legally happen, if the underlying Java hash function sent the edge IDs to the same
        // integer... but if that's what happens, we'd like to know about it
        assert(keyFunction.getEdgeKey(edge1) != keyFunction.getEdgeKey(oddEdgeOut));
    }

    @Test
    public void testGetVertexKey() throws Exception {

        DestinationVertexKeyFunction keyFunction = new DestinationVertexKeyFunction();

        StringType name      = new StringType("Tex, Ver Tex.");
        StringType otherName = new StringType("huh?");


        Vertex<StringType> vertex       = new Vertex<StringType>(name);
        Vertex<StringType> vertexClone  = new Vertex<StringType>(name);
        Vertex<StringType> oddVertexOut = new Vertex<StringType>(otherName);

        assertEquals(keyFunction.getVertexKey(vertex), keyFunction.getVertexKey(vertexClone));

        // technically, the two vertices could have the same key value, if the underlying Java hash function sent their
        // IDs to the same integer... but if that's the case we'd like to know about it

        assert(keyFunction.getVertexKey(vertex) != keyFunction.getVertexKey(oddVertexOut));
    }

}

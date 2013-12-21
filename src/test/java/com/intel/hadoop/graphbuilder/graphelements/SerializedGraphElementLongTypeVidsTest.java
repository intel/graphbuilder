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

import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.junit.Test;

import static junit.framework.Assert.*;

public class SerializedGraphElementLongTypeVidsTest {

    @Test
    public void testCreateVid() {
        SerializedGraphElementLongTypeVids elt = new SerializedGraphElementLongTypeVids();

        assertNotNull(elt);
        assertNull(elt.graphElement());

        Object vid = elt.createVid();

        assertNotNull(vid);

        assertEquals(vid.getClass(), VertexID.class);

    }

    @Test
    public void testCompareTo() {

        // Check the false case for Vertex class
        PropertyMap map0 = new PropertyMap();
        map0.setProperty("name", new StringType("Alice"));
        map0.setProperty("age", new LongType(30L));
        map0.setProperty("phone", new LongType(1234567890L));

        PropertyMap map1 = new PropertyMap();
        map1.setProperty("name", new StringType("Bob"));
        map1.setProperty("age", new LongType(40L));
        map1.setProperty("phone", new LongType(9876543210L));

        Vertex<LongType> vertex0 = new Vertex<LongType>(
                new LongType(001L),
                new StringType("IntelEmployee"),
                map0);

        SerializedGraphElementLongTypeVids element0 = new SerializedGraphElementLongTypeVids();
        element0.init(vertex0);

        Vertex<LongType> vertex1 = new Vertex<LongType>(
                new LongType(002L),
                new StringType("IntelManager"),
                map1);

        SerializedGraphElementLongTypeVids element1 = new SerializedGraphElementLongTypeVids();
        element1.init(vertex1);

        assertEquals(element0.compareTo(element1), 1);

        // Check the true case for Vertex class
        Vertex<LongType> vertex2 = new Vertex<LongType>(
                new LongType(001L),
                new StringType("IntelEmployee"),
                map0);

        SerializedGraphElementLongTypeVids element2 = new SerializedGraphElementLongTypeVids();
        element2.init(vertex2);

        assertEquals(element2.compareTo(element0), 0);

        // Check the false Edge class
        Edge<LongType> edge0 = new Edge<LongType>(
                new LongType(001L), null,
                new LongType(002L), null,
                new StringType("isConnected"),
                map0);

        SerializedGraphElementLongTypeVids element4 = new SerializedGraphElementLongTypeVids();
        element4.init(edge0);

        Edge<LongType> edge1 = new Edge<LongType>(
                new LongType(003L), null,
                new LongType(004L), null,
                new StringType("isConnected"),
                map1);

        SerializedGraphElementLongTypeVids element5 = new SerializedGraphElementLongTypeVids();
        element5.init(edge1);

        assert(element4.compareTo(element5) != 0);
        assert(element4.compareTo(element5) == (-1) * element5.compareTo(element4));

        // Check the true Edge class
        Edge<LongType> edge2 = new Edge<LongType>(
                new LongType(001L),  null,
                new LongType(002L),  null,
                new StringType("isConnected"),
                map0);

        SerializedGraphElementLongTypeVids element6 = new SerializedGraphElementLongTypeVids();
        element6.init(edge2);

        assertEquals(element6.compareTo(element4), 0);
    }
}

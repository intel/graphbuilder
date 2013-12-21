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

import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.junit.Test;

import static junit.framework.Assert.*;

public class SerializedGraphElementStringTypeVidsTest {
    @Test
    public void testCreateVid() {
        SerializedGraphElementStringTypeVids elt = new SerializedGraphElementStringTypeVids();

        assertNotNull(elt);
        assertNull(elt.graphElement());

        Object vid = elt.createVid();

        assertNotNull(vid);

        assertEquals(vid.getClass(), VertexID.class);
    }

    @Test
    public void testToString() {

        // expecting that the vertex ID show up in the string representation of a vertex property graph element
        // and that the source ID, destination ID and label show up in the string representation of an edge property
        // graph element seems like a reasonable expectation

        String     name = "veni VID-i vici";
        StringType vid  = new StringType(name);

        Vertex<StringType> vertex = new Vertex<StringType>(vid);

        SerializedGraphElementStringTypeVids vertexElement  = new SerializedGraphElementStringTypeVids();

        vertexElement.init(vertex);

        assert(vertexElement.graphElement().toString().contains(name));


        StringType srcName =  new StringType("The Source");
        StringType srcLabel = null;

        StringType dstName = new StringType("Destination Unkown");
        StringType dstLabel= new StringType("vlabel");

        VertexID<StringType> srcId = new VertexID<StringType>(srcName, srcLabel);
        VertexID<StringType> dstId = new VertexID<StringType>(dstName, dstLabel);

        String label   = "no labels, please";
        StringType wrappedLabel = new StringType(label);

        Edge<StringType> edge = new Edge<StringType>(srcId, dstId, wrappedLabel);

        SerializedGraphElementStringTypeVids edgeElement  = new SerializedGraphElementStringTypeVids();

        edgeElement.init(edge);

        assert(edgeElement.graphElement().toString().contains(srcName.toString()));
        assert(edgeElement.graphElement().toString().contains(dstName.toString()));
        assert(edgeElement.graphElement().toString().contains(dstLabel.toString()));
        assert(edgeElement.graphElement().toString().contains(label));

        // as for the null graph element...
        // well, I don't care what you call it, but it needs to have nonzero length string

        SerializedGraphElementStringTypeVids nullElement = new SerializedGraphElementStringTypeVids();

        assert(nullElement.toString().length() > 0);
    }

    @Test
    public void testCompareTo() {

        // Check the false case for Vertex class
        PropertyMap map0 = new PropertyMap();
        map0.setProperty("name", new StringType("Alice"));
        map0.setProperty("age", new StringType("30"));
        map0.setProperty("phone", new StringType("1234567890"));

        Vertex<StringType> vertex0 = new Vertex<StringType>(new StringType("Employee001"),
                new StringType("IntelEmployee"), map0);

        SerializedGraphElementStringTypeVids element0 = new SerializedGraphElementStringTypeVids();
        element0.init(vertex0);

        PropertyMap map1 = new PropertyMap();
        map1.setProperty("name", new StringType("Bob"));
        map1.setProperty("age", new StringType("40"));
        map1.setProperty("phone", new StringType("9876543210"));

        Vertex<StringType> vertex1 = new Vertex<StringType>(new StringType("Employee002"),
                new StringType("IntelManager"), map1);

        SerializedGraphElementStringTypeVids element1 = new SerializedGraphElementStringTypeVids();
        element1.init(vertex1);

        assert(element0.compareTo(element1) != 0);
        assert(element0.compareTo(element1) == (-1) * element1.compareTo(element0));

        // Check the true case for Vertex class
        Vertex<StringType> vertex2 = new Vertex<StringType>(new StringType("Employee001"),
                new StringType("IntelEmployee"), map0);

        SerializedGraphElementStringTypeVids element2 = new SerializedGraphElementStringTypeVids();
        element2.init(vertex2);

        assertEquals(element2.compareTo(element0), 0);

        StringType dummyVertexLabel = null;
        // Check the false Edge class
        Edge<StringType> edge0 = new Edge<StringType>(
                new StringType("Employee001"), dummyVertexLabel,
                new StringType("Employee002"), dummyVertexLabel,
                new StringType("isConnected"),
                map0);

        SerializedGraphElementStringTypeVids element4 = new SerializedGraphElementStringTypeVids();
        element4.init(edge0);

        Edge<StringType> edge1 = new Edge<StringType>(
                new StringType("Employee003"), dummyVertexLabel,
                new StringType("Employee004"), dummyVertexLabel,
                new StringType("isConnected"),
                map1);

        SerializedGraphElementStringTypeVids element5 = new SerializedGraphElementStringTypeVids();
        element5.init(edge1);

        assert(element4.compareTo(element5) != 0);
        assert(element4.compareTo(element5) ==  (-1) * element5.compareTo(element4));

        // Check the true Edge class
        Edge<StringType> edge2 = new Edge<StringType>(
                new StringType("Employee001"), dummyVertexLabel,
                new StringType("Employee002"), dummyVertexLabel,
                new StringType("isConnected"),
                map0);

        SerializedGraphElementStringTypeVids element6 = new SerializedGraphElementStringTypeVids();
        element6.init(edge2);

        assertEquals(element6.compareTo(element4), 0);
    }
}

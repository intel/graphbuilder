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

import com.intel.hadoop.graphbuilder.types.IntType;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.junit.Test;

import java.io.*;

import static junit.framework.Assert.*;

public class VertexTest {

    @Test
    public final void testNoArgConstructor() {
        Vertex<StringType> vertex = new Vertex<StringType>();

        VertexID<StringType> nullId = new VertexID<StringType>(null,null);
        assertNotNull(vertex);
        assertEquals(vertex.getId(), nullId);
        assertNotNull(vertex.getProperties());
    }

    @Test
    public final void testConstructorWithArgs() {
        StringType vertexName = new StringType("The Greatest Vertex EVER");
        VertexID<StringType>  vertexId = new VertexID<StringType>(vertexName);

        Vertex<StringType> vertex = new Vertex<StringType>(vertexName);

        assertNotNull(vertex);
        assert (vertex.getId().equals(vertexId));
        assertNotNull(vertex.getProperties());
    }

    @Test
    public final void testConfigureWithGetters() {

        StringType vertexName = new StringType("The Greatest Vertex EVER");
        StringType vertexLabel = new StringType("label");

        VertexID<StringType>  vertexId = new VertexID<StringType>(vertexName, vertexLabel);

        Vertex<StringType> vertex = new Vertex<StringType>(vertexName, vertexLabel);

        assertNotNull(vertex);
        assert (vertex.getId().equals(vertexId));
        assertNotNull(vertex.getProperties());

        PropertyMap pm = vertex.getProperties();
        PropertyMap pm2 = new PropertyMap();

        StringType anotherOpinion = new StringType("No that vertex sucks");

        VertexID<StringType>  diffNameSameLabel = new VertexID<StringType>(anotherOpinion, vertexLabel);

        vertex.configure(diffNameSameLabel, pm2);
        assert(vertex.getId().equals(diffNameSameLabel));
        assertSame(vertex.getProperties(), pm2);

        vertex.configure(vertexId, pm);
        assert (vertex.getId().equals(vertexId));
        assertSame(vertex.getProperties(), pm);
    }

    @Test
    public final void testGetSetProperty() {
        StringType vertexId = new StringType("The Greatest Vertex EVER");
        Vertex<StringType> vertex = new Vertex<StringType>(vertexId);

        String key1 = new String("key");
        String key2 = new String("Ce n'est pas une clé");

        StringType value1 = new StringType("Outstanding Value");
        StringType value2 = new StringType("Little Value");

        assert (vertex.getProperties().getPropertyKeys().isEmpty());

        vertex.setProperty(key1, value1);
        assertSame(vertex.getProperty(key1), value1);
        assertNull(vertex.getProperty(key2));

        vertex.setProperty(key2, value2);
        assertSame(vertex.getProperty(key1), value1);
        assertSame(vertex.getProperty(key2), value2);

        vertex.setProperty(key1, value2);
        assertSame(vertex.getProperty(key1), value2);
        assertSame(vertex.getProperty(key2), value2);

        vertex.setProperty(key2, value1);
        assertSame(vertex.getProperty(key1), value2);
        assertSame(vertex.getProperty(key2), value1);

        assert (vertex.getProperties().getPropertyKeys().size() == 2);
    }

    @Test
    public final void testToString() {
        StringType id1 = new StringType("the greatest vertex ID ever");
        StringType id2 = new StringType("worst vertex ID ever");

        Vertex<StringType> vertex1 = new Vertex<StringType>(id1);
        Vertex<StringType> vertex2 = new Vertex<StringType>(id2);

        Vertex<StringType> vertex3 = new Vertex<StringType>(id1);

        assertNotNull(vertex1.toString());
        assertNotNull(vertex2.toString());
        assertNotNull(vertex3.toString());

        assert (vertex1.toString().compareTo(vertex2.toString()) != 0);
        assert (vertex1.toString().compareTo(vertex3.toString()) == 0);

        String key = new String("key");
        StringType value = new StringType("bank");

        vertex1.setProperty(key, value);

        assert (vertex1.toString().compareTo(vertex2.toString()) != 0);
        assert (vertex1.toString().compareTo(vertex3.toString()) != 0);
    }

    @Test
    public final void testWriteRead() throws IOException {
        StringType id = new StringType("the greatest vertex of ALLL TIIIME!!!");
        Vertex<StringType> vertex = new Vertex<StringType>(id);

        Vertex<StringType> vertexOnTheOtherEnd = new Vertex<StringType>(new StringType("maybe not so much"));

        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataOutputStream dataOutputStream = new DataOutputStream(baos);

        vertex.write(dataOutputStream);
        dataOutputStream.flush();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(bais);

        vertexOnTheOtherEnd.readFields(dataInputStream);

        assert (vertex.getId().equals(vertexOnTheOtherEnd.getId()));
        assert (vertex.getProperties().toString().equals(vertexOnTheOtherEnd.getProperties().toString()));

        // one more time, with a nonempty property list

        String key = new String("key");
        IntType value = new IntType(666);

        vertex.setProperty(key, value);

        ByteArrayOutputStream baos2 = new ByteArrayOutputStream(1024);
        DataOutputStream dataOutputStream2 = new DataOutputStream(baos2);

        vertex.write(dataOutputStream2);
        dataOutputStream.flush();

        ByteArrayInputStream bais2 = new ByteArrayInputStream(baos2.toByteArray());
        DataInputStream dataInputStream2 = new DataInputStream(bais2);

        vertexOnTheOtherEnd.readFields(dataInputStream2);

        assert (vertex.getId().equals(vertexOnTheOtherEnd.getId()));
        assert (vertex.getProperties().toString().equals(vertexOnTheOtherEnd.getProperties().toString()));
    }

    @Test
    public void testEquals() {

        PropertyMap map0 = new PropertyMap();
        map0.setProperty("name", new StringType("Alice"));
        map0.setProperty("age", new StringType("30"));
        map0.setProperty("dept", new StringType("IntelCorp"));

        PropertyMap map1 = new PropertyMap();
        map1.setProperty("name", new StringType("Bob"));
        map1.setProperty("age", new StringType("32"));
        map1.setProperty("dept", new StringType("IntelLabs"));

        Vertex<StringType> vertex0 = new Vertex<StringType>(
                new StringType("Employee001"),
                new StringType("Rockstar"),
                map0);
        Vertex<StringType> vertex1 = new Vertex<StringType>(
                new StringType("Employee002"),
                new StringType("Failure"),
                map1);
        Vertex<StringType> vertex2 = new Vertex<StringType>(
                new StringType("Employee001"),
                new StringType("Rockstar"),
                map0);

        assertFalse("Vertex equality check failed", vertex0.equals(vertex1));
        assertTrue("Vertex equality check failed", vertex0.equals(vertex2));
    }
}

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

import java.io.*;

import static junit.framework.Assert.*;

public class EdgeTest {

    @Test
    public final void testConstructorWithoutArgs() {
        Edge<StringType> edge = new Edge<StringType>();

        assertNotNull(edge);
        assertNull(edge.getSrc());
        assertNull(edge.getDst());
        assertNull(edge.getLabel());
        assertNotNull(edge.getId());
        assertNotNull(edge.getProperties());
    }

    @Test
    public final void testConstructorWithArgs() {
        StringType src = new StringType("src");
        StringType dst = new StringType("dst");
        StringType label = new StringType("label");

        Edge<StringType> edge = new Edge<StringType>(src, dst, label);

        assertSame(src, edge.getSrc().getName());
        assertSame(dst, edge.getDst().getName());
        assertSame(label, edge.getLabel());
        assertNotNull(edge.getId());
        assertNotNull(edge.getProperties());
    }

    @Test
    public final void testConfigure() {
        StringType srcName = new StringType("src");
        VertexID<StringType> srcId = new VertexID<StringType>(srcName, null);

        StringType dstName = new StringType("dst");
        VertexID<StringType>  dstId = new VertexID<StringType>(dstName, null);

        StringType label = new StringType("label");
        PropertyMap pm = new PropertyMap();

        Edge<StringType> edge = new Edge<StringType>();

        edge.configure(srcId, dstId, label, pm);

        assertSame(srcId, edge.getSrc());
        assertSame(dstId, edge.getDst());
        assertSame(label, edge.getLabel());
        assertSame(pm, edge.getProperties());

        // now test against an edge created with arguments in the constructor

        StringType srcName2 = new StringType("src2");
        VertexID<StringType> srcId2 = new VertexID<StringType>(srcName2, null);
        StringType dstName2 = new StringType("dst2");
        VertexID<StringType> dstId2 = new VertexID<StringType>(dstName2, null);
        StringType label2 = new StringType("label2");

        Edge<StringType> edge2 = new Edge<StringType>(srcId2, dstId2, label2);

        edge2.configure(srcId, dstId, label, pm);

        assertSame(srcId, edge2.getSrc());
        assertSame(dstId, edge2.getDst());
        assertSame(label, edge2.getLabel());
        assertSame(pm, edge2.getProperties());
    }

    @Test
    public final void testProperties() {
        StringType src = new StringType("src");
        VertexID<StringType> srcId = new VertexID<StringType>(src, null);
        StringType dst = new StringType("dst");
        VertexID<StringType> dstId = new VertexID<StringType>(dst,null);
        StringType label = new StringType("label");

        String key1 = new String("key");
        String key2 = new String("Ce n'est pas une clé");

        StringType value1 = new StringType("Outstanding Value");
        StringType value2 = new StringType("Little Value");

        Edge<StringType> edge = new Edge<StringType>(srcId, dstId, label);

        assert (edge.getProperties().getPropertyKeys().isEmpty());

        edge.setProperty(key1, value1);
        edge.setProperty(key2, value2);

        assertSame(value1, edge.getProperty(key1));
        assertSame(value1, edge.getProperties().getProperty(key1));

        assertSame(value2, edge.getProperty(key2));
        assertSame(value2, edge.getProperties().getProperty(key2));

        edge.setProperty(key1, value2);
        edge.setProperty(key2, value1);

        assertSame(value2, edge.getProperty(key1));
        assertSame(value2, edge.getProperties().getProperty(key1));

        assertSame(value1, edge.getProperty(key2));
        assertSame(value1, edge.getProperties().getProperty(key2));

        assert (edge.getProperties().getPropertyKeys().size() == 2);
    }

    @Test
    public final void testGetEdgeID() {
        StringType src1 = new StringType("src1");
        VertexID<StringType> srcId1 = new VertexID<StringType>(src1, null);
        StringType dst1 = new StringType("dst1");
        VertexID<StringType> dstId1 = new VertexID<StringType>(dst1, null);
        StringType label1 = new StringType("label1");

        StringType src2 = new StringType("src2");
        VertexID<StringType> srcId2 = new VertexID<StringType>(src2, null);
        StringType dst2 = new StringType("dst2");
        VertexID<StringType> dstId2 = new VertexID<StringType>(dst2, null);
        StringType label2 = new StringType("label2");

        Edge<StringType> edge1 = new Edge<>(srcId1, dstId1, label1);
        Edge<StringType> edge2 = new Edge<>(srcId2, dstId2, label2);
        Edge<StringType> edge3 = new Edge<>(srcId1, dstId1, label1);

        assertNotNull(edge1.getId());
        assertNotNull(edge2.getId());
        assertNotNull(edge3.getId());

        assertFalse(edge1.getId().equals(edge2.getId()));
        assert (edge1.getId().equals(edge3.getId()));

        String key = new String("key");
        StringType value = new StringType("bank");

        edge1.setProperty(key, value);
        assertFalse(edge1.getId().equals(edge2.getId()));
        assert (edge1.getId().equals(edge3.getId()));
    }

    @Test
    public final void testToString() {
        StringType src1 = new StringType("src1");
        VertexID<StringType> srcId1 = new VertexID<StringType>(src1, null);
        StringType dst1 = new StringType("dst1");
        VertexID<StringType> dstId1 = new VertexID<StringType>(dst1, null);
        StringType label1 = new StringType("label1");

        StringType src2 = new StringType("src2");
        VertexID<StringType> srcId2 = new VertexID<StringType>(src2, null);
        StringType dst2 = new StringType("dst2");
        VertexID<StringType> dstId2 = new VertexID<StringType>(dst2, null);
        StringType label2 = new StringType("label2");

        Edge<StringType> edge1 = new Edge<>(srcId1, dstId1, label1);
        Edge<StringType> edge2 = new Edge<>(srcId2, dstId2, label2);
        Edge<StringType> edge3 = new Edge<>(srcId1, dstId1, label1);

        assertNotNull(edge1.toString());
        assertNotNull(edge2.toString());
        assertNotNull(edge3.toString());

        assertFalse(edge1.toString().equals(edge2.toString()));
        assert (edge1.toString().equals(edge3.toString()));

        String key = new String("key");
        StringType value = new StringType("bank");

        edge1.setProperty(key, value);
        assertFalse(edge1.toString().equals(edge2.toString()));
        assertFalse(edge1.toString().equals(edge3.toString()));
    }

    @Test
    public final void testSelfEdge() {
        StringType src = new StringType("src");
        StringType dst = new StringType("dst");
        StringType label = new StringType("label");

        Edge<StringType> nonLoop = new Edge<StringType>(src, dst, label);
        Edge<StringType> loop = new Edge<StringType>(src, src, label);

        assertFalse(nonLoop.isSelfEdge());
        assert (loop.isSelfEdge());
    }

    @Test
    public final void testWriteRead() throws IOException

    {
        StringType src = new StringType("src");
        StringType dst = new StringType("dst");
        StringType label = new StringType("label");

        Edge<StringType> edge = new Edge<StringType>(src, dst, label);

        StringType src2 = new StringType("src2");
        StringType dst2 = new StringType("dst2");
        StringType label2 = new StringType("label2");
        Edge<StringType> edgeOnTheOtherEnd = new Edge<StringType>(src2, dst2, label2);

        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataOutputStream dataOutputStream = new DataOutputStream(baos);

        edge.write(dataOutputStream);
        dataOutputStream.flush();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(bais);

        edgeOnTheOtherEnd.readFields(dataInputStream);

        assert (edgeOnTheOtherEnd.getId().equals(edge.getId()));
        assert (edgeOnTheOtherEnd.getProperties().toString().equals(edge.getProperties().toString()));

        // one more time, with a nonempty property list

        String key = new String("key");
        StringType value = new StringType("bank");

        edge.setProperty(key, value);

        ByteArrayOutputStream baos2 = new ByteArrayOutputStream(1024);
        DataOutputStream dataOutputStream2 = new DataOutputStream(baos2);

        edge.write(dataOutputStream2);
        dataOutputStream.flush();

        ByteArrayInputStream bais2 = new ByteArrayInputStream(baos2.toByteArray());
        DataInputStream dataInputStream2 = new DataInputStream(bais2);

        edgeOnTheOtherEnd.readFields(dataInputStream2);

        assert (edgeOnTheOtherEnd.getId().equals(edge.getId()));
        assert (edgeOnTheOtherEnd.getProperties().toString().equals(edge.getProperties().toString()));
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

        Edge<StringType> edge0 = new Edge<StringType>(
                new StringType("Employee001"), new StringType("Employee002"),
                new StringType("isConnected"),
                map0);

        Edge<StringType> edge1 = new Edge<StringType>(
                new StringType("Employee002"),
                new StringType("Employee003"),
                new StringType("likes"),
                map1);
        Edge<StringType> edge2 = new Edge<StringType>(
                new StringType("Employee001"),
                new StringType("Employee002"),
                new StringType("isConnected"),
                map0);

        assertFalse("Edge equality check failed", edge0.equals(edge1));
        assertTrue("Edge equality check failed", edge0.equals(edge2));
    }
}

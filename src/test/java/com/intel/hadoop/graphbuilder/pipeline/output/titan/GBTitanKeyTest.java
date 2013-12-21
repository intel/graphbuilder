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

import static junit.framework.Assert.assertSame;

import org.junit.Test;

public class GBTitanKeyTest {

    @Test
    public void constructorSetGetTest() throws Exception {
        String   name        = "name";

        GBTitanKey key1 = new GBTitanKey(name);

        assertSame(key1.getName(), name);
        assertSame(key1.getDataType(), String.class);
        assert(key1.isEdgeIndex() == false);
        assert(key1.isVertexIndex() == false);
        assert(key1.isUnique() == false);

        Class<?> dataType    = Float.class;
        boolean  vertexIndex = false;
        boolean  edgeIndex   = false;
        boolean  unique      = false;

        GBTitanKey key2 = new GBTitanKey(name, dataType, edgeIndex, vertexIndex, unique);

        assertSame(key2.getName(), name);
        assertSame(key2.getDataType(), dataType);
        assert(key2.isEdgeIndex() == edgeIndex);
        assert(key2.isVertexIndex() == vertexIndex);
        assert(key2.isUnique() == unique);

        String   name1        = "name1";
        Class<?> dataType1    = Integer.class;
        boolean  vertexIndex1 = true;
        boolean  edgeIndex1   = true;
        boolean  unique1      = true;

        key2.setName(name1);
        assertSame(key2.getName(), name1);
        assertSame(key2.getDataType(), dataType);
        assert(key2.isEdgeIndex() == edgeIndex);
        assert(key2.isVertexIndex() == vertexIndex);
        assert(key2.isUnique() == unique);

        key2.setDataType(dataType1);
        assertSame(key2.getName(), name1);
        assertSame(key2.getDataType(), dataType1);
        assert(key2.isEdgeIndex() == edgeIndex);
        assert(key2.isVertexIndex() == vertexIndex);
        assert(key2.isUnique() == unique);

        key2.setIsEdgeIndex(edgeIndex1);
        assertSame(key2.getName(), name1);
        assertSame(key2.getDataType(), dataType1);
        assert(key2.isEdgeIndex() == edgeIndex1);
        assert(key2.isVertexIndex() == vertexIndex);
        assert(key2.isUnique() == unique);

        key2.setIsVertexIndex(vertexIndex1);
        assertSame(key2.getName(), name1);
        assertSame(key2.getDataType(), dataType1);
        assert(key2.isEdgeIndex() == edgeIndex1);
        assert(key2.isVertexIndex() == vertexIndex1);
        assert(key2.isUnique() == unique);

        key2.setIsUnique(unique1);
        assertSame(key2.getName(), name1);
        assertSame(key2.getDataType(), dataType1);
        assert(key2.isEdgeIndex() == edgeIndex1);
        assert(key2.isVertexIndex() == vertexIndex1);
        assert(key2.isUnique() == unique1);

        key2.setName(name);
        assertSame(key2.getName(), name);
        assertSame(key2.getDataType(), dataType1);
        assert(key2.isEdgeIndex() == edgeIndex1);
        assert(key2.isVertexIndex() == vertexIndex1);
        assert(key2.isUnique() == unique1);

        key2.setDataType(dataType);
        assertSame(key2.getName(), name);
        assertSame(key2.getDataType(), dataType);
        assert(key2.isEdgeIndex() == edgeIndex1);
        assert(key2.isVertexIndex() == vertexIndex1);
        assert(key2.isUnique() == unique1);

        key2.setIsEdgeIndex(edgeIndex);
        assertSame(key2.getName(), name);
        assertSame(key2.getDataType(), dataType);
        assert(key2.isEdgeIndex() == edgeIndex);
        assert(key2.isVertexIndex() == vertexIndex1);
        assert(key2.isUnique() == unique1);

        key2.setIsVertexIndex(vertexIndex);
        assertSame(key2.getName(), name);
        assertSame(key2.getDataType(), dataType);
        assert(key2.isEdgeIndex() == edgeIndex);
        assert(key2.isVertexIndex() == vertexIndex);
        assert(key2.isUnique() == unique1);

        key2.setIsUnique(unique);
        assertSame(key2.getName(), name);
        assertSame(key2.getDataType(), dataType);
        assert(key2.isEdgeIndex() == edgeIndex);
        assert(key2.isVertexIndex() == vertexIndex);
        assert(key2.isUnique() == unique);
    }
}

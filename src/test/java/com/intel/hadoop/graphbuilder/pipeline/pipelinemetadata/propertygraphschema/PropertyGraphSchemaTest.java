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
package com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.propertygraphschema;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNotSame;
import static junit.framework.Assert.assertSame;

import java.util.HashMap;

import org.junit.Test;

public class PropertyGraphSchemaTest {

    @Test
    public void testPropertyGraphSchemaConstructor() {

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();

        assertNotNull(graphSchema.getVertexSchemata());
        assertNotNull(graphSchema.getEdgeSchemata());

        assertNotSame(graphSchema.getEdgeSchemata(), graphSchema.getVertexSchemata());
    }

    @Test
    public void testAddVertexSchema() {

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();

        VertexSchema vertexSchema = new VertexSchema();

        graphSchema.addVertexSchema(vertexSchema);

        assert(graphSchema.getVertexSchemata().contains(vertexSchema));
    }

    @Test
    public void testAddEdgeSchema() {

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();

        EdgeSchema edgeSchema = new EdgeSchema("foo");

        graphSchema.addEdgeSchema(edgeSchema);

        assert(graphSchema.getEdgeSchemata().contains(edgeSchema));
    }

    @Test
    public void testGetMapOfPropertyNamesToDataTypes() {

        final String PLANET_OF_THE_STRINGS = "planet of the strings";
        final String PLANET_OF_THE_FLOATS  = "planet of the floags";
        final String PLANET_OF_THE_LONGS   = "long and strong and down to get some testin on";

        PropertySchema planetOfStrings = new PropertySchema(PLANET_OF_THE_STRINGS, String.class);
        PropertySchema planetOfFloats  = new PropertySchema(PLANET_OF_THE_FLOATS, Float.class);

        EdgeSchema edgeSchemaZ = new EdgeSchema("dr zaius");

        edgeSchemaZ.setLabel("you d--- dirty ape");
        edgeSchemaZ.getPropertySchemata().add(planetOfStrings);

        VertexSchema vertexSchema = new VertexSchema();
        vertexSchema.getPropertySchemata().add(planetOfFloats);

        PropertyGraphSchema graphSchema = new PropertyGraphSchema();

        graphSchema.addVertexSchema(vertexSchema);
        graphSchema.addEdgeSchema(edgeSchemaZ);

        HashMap map = graphSchema.getMapOfPropertyNamesToDataTypes();

        assertSame(map.get(PLANET_OF_THE_FLOATS), Float.class);
        assertSame(map.get(PLANET_OF_THE_STRINGS), String.class);
        assertSame(map.get(PLANET_OF_THE_LONGS), null) ;
    }
}

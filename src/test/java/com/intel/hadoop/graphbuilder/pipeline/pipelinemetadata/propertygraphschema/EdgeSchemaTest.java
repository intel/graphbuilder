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

import org.junit.Test;


public class EdgeSchemaTest {

    @Test
    public void edgeSchemaConstructorTest() throws Exception {

        final String THE_EDGE = "The Edge";
        final String BONO     = "Bono";

        EdgeSchema edgeSchema = new EdgeSchema(THE_EDGE);

        assertNotNull(edgeSchema.getPropertySchemata());
        assert(edgeSchema.getLabel().compareTo(THE_EDGE) == 0);
    }

    @Test
    public void edgeSchemaSetGetLabelTest() {

        final String THE_EDGE = "The Edge";
        final String BONO     = "Bono";

        EdgeSchema edgeSchema = new EdgeSchema(THE_EDGE);

        assert(edgeSchema.getLabel().compareTo(THE_EDGE) == 0);

        edgeSchema.setLabel(BONO);
        assert(edgeSchema.getLabel().compareTo(BONO) == 0);

        edgeSchema.setLabel(THE_EDGE);
        assert(edgeSchema.getLabel().compareTo(THE_EDGE) == 0);
    }

}

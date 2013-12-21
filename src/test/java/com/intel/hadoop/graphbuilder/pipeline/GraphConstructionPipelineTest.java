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
package com.intel.hadoop.graphbuilder.pipeline;

import java.lang.reflect.Field;
import java.util.HashMap;

import org.junit.Test;

public class GraphConstructionPipelineTest {
    @Test
    public void testAddUserOpt() throws Exception {
        GraphConstructionPipeline pipeline = new GraphConstructionPipeline();

        String key   = "key";
        String value = "value";

        pipeline.addUserOpt(key, value);

        Field privateOptField = GraphConstructionPipeline.class.
                getDeclaredField("userOpts");

        privateOptField.setAccessible(true);

        HashMap opts = (HashMap) privateOptField.get(pipeline);
        assert(value.equals(opts.get(key)));
    }

    @Test
    public void testRun() throws Exception {

    }
}

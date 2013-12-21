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
package com.intel.hadoop.graphbuilder.pipeline.output;

import com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphBuildingRule;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.HashMap;

/**
 * These are the methods that must be provided by the (chained) Map Reducer 
 * job(s) for generating a graph.
 * @see  com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanWriterMRChain
 */
public abstract class GraphGenerationMRJob {
    public abstract void setValueClass(Class valueClass);
    public abstract void setVidClass(Class vidClass);
    public abstract void setCleanBidirectionalEdges(boolean clean);
    public abstract void setUserOptions(HashMap<String, String> userOpts);
    public abstract void init(InputConfiguration inputConfiguration, GraphBuildingRule graphBuildingRule);
    public abstract void run(CommandLine cmd) throws IOException, ClassNotFoundException, InterruptedException;
}

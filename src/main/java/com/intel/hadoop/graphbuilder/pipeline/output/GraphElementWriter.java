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

import com.intel.hadoop.graphbuilder.graphelements.EdgeID;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.pipeline.pipelinemetadata.keyfunction.KeyFunction;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.ArgumentBuilder;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Hashtable;

/**
 * This class provides a simple interface for writing the merged edges 
 * and vertices.
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.output.titan.TitanGraphElementWriter
 */
public abstract class GraphElementWriter {
    protected Hashtable<EdgeID, Writable> edgeSet;
    protected Hashtable<Object, Writable> vertexSet;
    protected Hashtable<Object, StringType> vertexLabelMap;
    protected Enum vertexCounter;
    protected Enum edgeCounter;
    protected Reducer.Context context;
    protected TitanGraph graph;
    protected SerializedGraphElement outValue;
    protected IntWritable outKey;
    protected KeyFunction keyFunction;

    protected  void initArgs(ArgumentBuilder args){
        edgeSet = (Hashtable<EdgeID, Writable>)args.get("edgeSet");
        vertexSet = (Hashtable<Object, Writable>)args.get("vertexSet");
        vertexLabelMap = (Hashtable<Object, StringType>)args.get("vertexLabelMap", new Hashtable<Object, StringType>());

        vertexCounter = (Enum)args.get("vertexCounter");
        edgeCounter = (Enum)args.get("edgeCounter");

        context = (Reducer.Context)args.get("context");

        graph = (TitanGraph)args.get("graph");

        outValue = (SerializedGraphElement)args.get("outValue");
        outKey = (IntWritable)args.get("outKey");
        keyFunction = (KeyFunction)args.get("keyFunction");
    }

    public abstract void write(ArgumentBuilder args) throws IOException, InterruptedException;

    public abstract void writeVertices(ArgumentBuilder args) throws IOException, InterruptedException;

    public abstract void writeEdges(ArgumentBuilder args) throws IOException, InterruptedException;

}

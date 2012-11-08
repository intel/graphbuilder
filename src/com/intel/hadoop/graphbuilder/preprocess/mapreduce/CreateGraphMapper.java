/* Copyright (C) 2012 Intel Corporation.
 *     All rights reserved.
 *           
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder 
 */
package com.intel.hadoop.graphbuilder.preprocess.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.graph.Edge;
import com.intel.hadoop.graphbuilder.graph.Vertex;
import com.intel.hadoop.graphbuilder.preprocess.inputformat.GraphTokenizer;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.VertexEdgeUnionType;

/**
 * The Mapper class parses each input value provided by the {@code InputFormat},
 * and outputs a list of {@code Vertex} and a list of {@code Edge} using a
 * {@code GraphTokenizer}.
 * 
 */
public class CreateGraphMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, IntWritable, VertexEdgeUnionType> {

  private static final Logger LOG = Logger.getLogger(CreateGraphMapper.class);

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    try {
      this.tokenizer = (GraphTokenizer) Class
          .forName(job.get("GraphTokenizer")).newInstance();
      tokenizer.configure(job);
      this.valClass = job.getMapOutputValueClass();
      mapVal = (VertexEdgeUnionType) valClass.newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void map(LongWritable key, Text value,
      OutputCollector<IntWritable, VertexEdgeUnionType> out, Reporter arg3)
      throws IOException {

    tokenizer.parse(value.toString());
    Iterator<Edge> eiter = tokenizer.getEdges();
    Iterator<Vertex> viter = tokenizer.getVertices();
    try {

      while (eiter.hasNext()) {
        Edge e = eiter.next();
        mapVal.init(VertexEdgeUnionType.EDGEVAL, e);
        out.collect(new IntWritable(e.hashCode()), mapVal);
      }

      while (viter.hasNext()) {
        Vertex v = viter.next();
        mapVal.init(VertexEdgeUnionType.VERTEXVAL, v);
        out.collect(new IntWritable(v.hashCode()), mapVal);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private VertexEdgeUnionType mapVal;
  private GraphTokenizer tokenizer;
  protected Class valClass;
}

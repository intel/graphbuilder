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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.graph.Edge;
import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.PairListType;

/**
 * This Mapper class maps each edge (u,v,data) into (u, (v, data)) or (v, (u,
 * data)) based on {@code reduceEndPoint} parameter in the JobConf configured in
 * {@code EdgeTrandFormMR}.
 * 
 * 
 * @param <VidType>
 */
public class EdgeTransformMapper<VidType extends WritableComparable<VidType>>
    extends MapReduceBase implements
    Mapper<LongWritable, Text, VidType, PairListType> {

  private static final Logger LOG = Logger.getLogger(EdgeTransformMapper.class);

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    this.reduceEndPoint = job.getBoolean("reduceEndPoint",
        EdgeTransformMR.SOURCE);
    try {
      this.graphparser = (GraphParser) Class.forName(job.get("GraphParser"))
          .newInstance();
      this.vidparser = (FieldParser) Class.forName(job.get("VidParser"))
          .newInstance();
      this.edataparser = (FieldParser) Class.forName(job.get("EdataParser"))
          .newInstance();
      this.valClass = job.getMapOutputValueClass();
      val = (PairListType) valClass.newInstance();
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
      OutputCollector<VidType, PairListType> out, Reporter report)
      throws IOException {
    String line = value.toString();
    try {

      if (graphparser.isEdgeData(line)) {
        Edge e = graphparser.parseEdge(line, vidparser, edataparser);
        if (reduceEndPoint == EdgeTransformMR.SOURCE) {
          val.init(e.target(), e.EdgeData());
          out.collect((VidType) e.source(), val);
        } else {
          val.init(e.source(), e.EdgeData());
          out.collect((VidType) e.target(), val);
        }
      } else {
        LOG.error("Skip line: " + line);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private PairListType val;
  private GraphParser graphparser;
  private FieldParser vidparser;
  private FieldParser edataparser;
  protected Class valClass;
  private boolean reduceEndPoint;
}

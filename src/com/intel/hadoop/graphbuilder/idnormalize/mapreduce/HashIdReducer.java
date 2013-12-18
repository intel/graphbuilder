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
package com.intel.hadoop.graphbuilder.idnormalize.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.graph.Vertex;
import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;

/**
 * Reducer class reduce the (baseid, List<(vid, vdata)>) into a dictionary
 * (lvid, vid), and a new vertex data file (lvid, vdata), where lvid = baseid +
 * vid.index * splitsize; Because the splitsize is fixed for all mapper using
 * option {@code mapred.line.input.format.linespermap} in the {@code JobConf},
 * this guarantee all vids are mapped into [0, ..., |V|-1]. The assumption is
 * that the input should not contain any duplicate vertex ids.
 * 
 */
public class HashIdReducer extends MapReduceBase implements
    Reducer<IntWritable, Text, Text, Text> {

  private static final Logger LOG = Logger.getLogger(HashIdReducer.class);

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    splitsize = job.getInt("mapred.line.input.format.linespermap", 6000000);
    //splitsize = job.getInt("mapreduce.input.lineinputformat.linespermap", 6000000);// for YARN
    try {
      this.graphparser = (GraphParser) Class.forName(job.get("GraphParser"))
          .newInstance();
      this.vidparser = (FieldParser) Class.forName(job.get("VidParser"))
          .newInstance();
      this.vdataparser = (FieldParser) Class.forName(job.get("VdataParser"))
          .newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void reduce(IntWritable key, Iterator<Text> iter,
      OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
    long baseid = key.get();
    int split = 0;
    while (iter.hasNext()) {
      Vertex v = graphparser.parseVertex(iter.next().toString(), vidparser,
          vdataparser);
      long newId = baseid + splitsize * split;
      out.collect(new Text("vidmap"), new Text(newId + "\t" + v.vid()));
      out.collect(new Text("vdata"), new Text("!" + newId + "\t"
          + v.vdata().toString()));
      split++;
    }
  }

  private int splitsize;
  private GraphParser graphparser;
  private FieldParser vidparser;
  private FieldParser vdataparser;
}

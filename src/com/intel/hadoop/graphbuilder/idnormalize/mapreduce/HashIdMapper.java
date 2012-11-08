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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.parser.GraphParser;

/**
 * This mapper class maps an (vid, vdata) pair into (lvid, (vid, vdata)) pair.
 * Each mapper gets fix lines of input, therefore the lvid is the relative line
 * offset of the record in the split, staring from 0. The split size is
 * configured in {@code mapred.line.input.format.linespermap}.
 * 
 */
public class HashIdMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, IntWritable, Text> {

  private static final Logger LOG = Logger.getLogger(HashIdMapper.class);

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    this.curId = 0;
    try {
      this.graphparser = (GraphParser) Class.forName(job.get("GraphParser"))
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
  public void map(LongWritable key, Text val,
      OutputCollector<IntWritable, Text> out, Reporter arg3) throws IOException {
    if (graphparser.isVertexData(val.toString())) {
      out.collect(new IntWritable(curId), new Text(val.toString()));
      ++curId;
    }
  }

  private GraphParser graphparser;
  private int curId;
}

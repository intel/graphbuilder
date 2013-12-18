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
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class SortDictReducer extends MapReduceBase implements
    Reducer<IntWritable, Text, Text, Text> {
  private MultipleOutputs mos;
  private boolean hashRawVid;

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    this.mos = new MultipleOutputs(job);
    this.hashRawVid = job.getBoolean("hashRawVid", true);
  }

  @Override
  public void reduce(IntWritable key, Iterator<Text> iter,
      OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
    OutputCollector<Text, Text> mout = mos.getCollector(
        "vidhashmap" + key.get(), reporter);
    while (iter.hasNext()) {
      Text line = iter.next();
      mout.collect(null, line);
    }
  }

  @Override
  public void close() throws IOException {
    mos.close();
  }

}
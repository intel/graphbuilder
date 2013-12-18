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
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.parser.FieldParser;

public class SortDictMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, IntWritable, Text> {

  private static final Logger LOG = Logger.getLogger(SortDictMapper.class);

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    this.hashRawVid = job.getBoolean("hashRawVid", true);
    this.numChunks = job.getInt("numChunks", 256);
    try {
      this.vidparser = (FieldParser) Class.forName(job.get("VidParser"))
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
      OutputCollector<IntWritable, Text> out, Reporter reporter)
      throws IOException {
    String line = val.toString();
    StringTokenizer tokenizer = new StringTokenizer(line);

    try {
      String vid = tokenizer.nextToken();
      if (hashRawVid) { // partition by old vid
        Object rawId = vidparser.getValue(tokenizer.nextToken());
        int hash = rawId.hashCode() % numChunks;
        if (hash < 0)
          hash += numChunks; // resolving negative hashcode
        out.collect(new IntWritable(hash), val);
      } else { // partition by new vid
        int hash = Long.valueOf(vid).hashCode() % numChunks;
        out.collect(new IntWritable(hash), val);
      }
    } catch (NoSuchElementException e) {
      e.printStackTrace();
      LOG.error("Error parsing vertex dictionary: " + val.toString());
    }

  }

  private FieldParser vidparser;
  private int numChunks;
  private boolean hashRawVid;
}
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
package com.intel.hadoop.graphbuilder.partition.mapreduce.vrecord;

import java.io.IOException;
import java.util.Iterator;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * This reducer class takes a list of {@code VertexRecord} that belongs to the
 * same partition, and write them into the output directory of that partition.
 * It also computes meta information about the number of vertices in the
 * partition as well as the number of vertices that have this partition as the
 * owner.
 * <p>
 * $outputdir/partition{$i}/vrecord contains the vertex records of partition i,
 * and $outputdir/partition{$i}/meta has the meta information of partition i.
 * </p>
 * 
 */
public class VrecordIngressReducer extends MapReduceBase implements
    Reducer<IntWritable, Text, Text, Text> {

  private static enum COUNTER {
    VERTICES, OWN_VERTICES
  };

  @Override
  public void configure(JobConf job) {
    super.configure(job);
  }

  @Override
  public void reduce(IntWritable key, Iterator<Text> value,
      OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
    int numVertices = 0;
    int numOwnVertices = 0;

    JSONParser parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE);
    while (value.hasNext()) {
      Text vrecString = value.next();
      JSONObject obj;
      try {
        obj = (JSONObject) parser.parse(vrecString.toString());
        int owner = ((Long) obj.get("owner")).intValue();
        if (owner == key.get()) {
          numOwnVertices++;
          reporter.incrCounter(COUNTER.OWN_VERTICES, 1);
        }
        out.collect(new Text("partition" + key.get() + " vrecord"), vrecString);
        numVertices++;
        reporter.incrCounter(COUNTER.VERTICES, 1);
      } catch (ParseException e) {
        e.printStackTrace();
      } // end try parsing
    } // end while

    JSONObject summary = new JSONObject();
    summary.put("numVertices", numVertices);
    summary.put("numOwnVertices", numOwnVertices);
    out.collect(new Text("partition" + key.get() + " meta"),
        new Text(summary.toJSONString()));
  }

  @Override
  public void close() throws IOException {
  }
}

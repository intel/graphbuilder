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

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.intel.hadoop.graphbuilder.graph.JsonVrecordFormatter;

/**
 * This mapper class map each {@code VertexRecord} into K records where K is the
 * size of the mirror plus 1 for the owner. The keys are the partition ids in
 * the mirrors and the owner.
 * <p>
 * Assuming the input vertex record was created using
 * {@code JsonVrecordFormatter}.
 * </p>
 * 
 * @see JsonVrecordFormatter
 */
public class VrecordIngressMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, IntWritable, Text> {

  @Override
  public void configure(JobConf job) {
    super.configure(job);
  }

  @Override
  public void map(LongWritable arg0, Text arg1,
      OutputCollector<IntWritable, Text> out, Reporter reporter)
      throws IOException {

    JSONParser parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE);
    JSONObject obj;
    try {
      String rec = arg1.toString();
      obj = (JSONObject) parser.parse(rec);

      short owner = ((Long) obj.get("owner")).shortValue();
      out.collect(new IntWritable(owner), new Text(rec));
      JSONArray mirrors = (JSONArray) obj.get("mirrors");
      for (int j = 0; j < mirrors.size(); j++) {
        out.collect(new IntWritable(((Long) mirrors.get(j)).intValue()),
            new Text(rec));
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
}

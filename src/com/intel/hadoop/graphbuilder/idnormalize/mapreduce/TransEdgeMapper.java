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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/**
 * This mapper class maps each edge as (u,v,data) into (h(v), (D(u), v, data))
 * where D is the dictionary that contains entry u. Because the input is
 * partitioned by the hash of source vertex, the number of dictionary loading is
 * minimized.
 * 
 * @param <VidType>
 */
public class TransEdgeMapper<VidType extends WritableComparable<VidType>>
    extends MapReduceBase implements
    Mapper<LongWritable, Text, IntWritable, Text> {

  private static final Logger LOG = Logger.getLogger(TransEdgeMapper.class);

  @Override
  public void configure(JobConf conf) {
    super.configure(conf);
    numChunks = conf.getInt("numChunks", 256);
    dictionaryPath = conf.get("dictionaryPath");
    dict = new HashMap<VidType, Long>();
    dictionaryId = -1;

    try {
      fs = FileSystem.get(conf);
    } catch (IOException e1) {
      e1.printStackTrace();
    }

    try {
      this.graphparser = (GraphParser) Class.forName(conf.get("GraphParser"))
          .newInstance();
      this.vidparser = (FieldParser) Class.forName(conf.get("VidParser"))
          .newInstance();
      this.edataparser = (FieldParser) Class.forName(conf.get("EdataParser"))
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
  public void map(LongWritable key, Text value,
      OutputCollector<IntWritable, Text> out, Reporter reporter)
      throws IOException {
    if (!graphparser.isEdgeData(value.toString()))
      return;

    Edge<VidType, ?> e = graphparser.parseEdge(value.toString(), vidparser,
        edataparser);
    int part = e.source().hashCode() % numChunks;
    if (part < 0)
      part += numChunks;
    if (part != dictionaryId) {
      dictionaryId = part;
      loadDictionary();
    }

    if (dict.containsKey(e.source())) {
      long srcId = dict.get(e.source());
      int targetHash = e.target().hashCode() % numChunks;
      if (targetHash < 0)
        targetHash += numChunks;
      Text output = new Text(srcId + "\t" + e.target().toString() + "\t"
          + e.EdgeData().toString());
      out.collect(new IntWritable(targetHash), output);
    } else {
      LOG.error("TransEdgeMapper: Cannot find key " + e.source().toString());
      LOG.error("Line: " + value.toString());
    }
  }

  /**
   * Load the dictionary partition with the current dictionaryId.
   * 
   * @throws IOException
   */
  private void loadDictionary() throws IOException {
    dict.clear();
    String prefix = "vidhashmap" + dictionaryId;
    FileStatus[] stats = fs.listStatus(new Path(dictionaryPath));
    for (FileStatus stat : stats) {
      if (stat.getPath().getName().matches(".*" + prefix + "-r-.*")) {
        LOG.debug(("Mapper Load dictionary: " + stat.getPath().getName()));
        Scanner sc = new Scanner(new BufferedReader(new InputStreamReader(
            fs.open(stat.getPath()))));
        while (sc.hasNextLine()) {
          String line = sc.nextLine();
          StringTokenizer tokenizer = new StringTokenizer(line);
          try {
            long id = Long.valueOf(tokenizer.nextToken());
            VidType rawid = vidparser.getValue(tokenizer.nextToken());
            dict.put(rawid, Long.valueOf(id));
          } catch (NoSuchElementException e) {
            e.printStackTrace();
            LOG.error("Error in loading vidmap entry:" + line);
          }
        }
      }
    }
  }

  GraphParser<VidType, ?, ?> graphparser;
  FieldParser<VidType> vidparser;
  FieldParser edataparser;

  int dictionaryId;
  HashMap<VidType, Long> dict;
  int numChunks;
  String dictionaryPath;
  FileSystem fs;
}

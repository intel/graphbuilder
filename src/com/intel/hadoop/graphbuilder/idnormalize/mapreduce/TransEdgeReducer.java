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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;

/**
 * This reducer class takes from mapper the input (h(v), [(D(u_i),v_i,
 * data)...]) and output (D(u_i), D(v_i), data). Because the key is based on
 * hash of the rawId, the loading of dictionary is minimized.
 * 
 * @param <VidType>
 */
public class TransEdgeReducer<VidType extends WritableComparable<VidType>>
    extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text> {

  private static final Logger LOG = Logger.getLogger(TransEdgeReducer.class);

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
  public void reduce(IntWritable key, Iterator<Text> iter,
      OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
    if (key.get() != dictionaryId) {
      dictionaryId = key.get();
      loadDictionary();
    }

    while (iter.hasNext()) {
      String line = iter.next().toString();
      StringTokenizer tk = new StringTokenizer(line);
      long sourceId = Long.valueOf(tk.nextToken());
      VidType target = vidparser.getValue(tk.nextToken());
      if (dict.containsKey(target)) {
        long targetId = dict.get(target);
        String edata = tk.hasMoreTokens() ? "\t" + tk.nextToken() : "";
        out.collect(null, new Text(sourceId + "\t" + targetId + edata));
      } else {
        LOG.error("Reducer: Cannot find key " + target.toString());
        LOG.error("Line: " + line);
      }
    }
  }

  /**
   * Load the dictionary partition with the current dictionaryId. Duplicate code
   * from {@code TransEdgeMapper#loadDictionary}.
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

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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.parser.FieldParser;

/**
 * This MapReduce class partitions the dictionary output of HashIdMR based on
 * the hash of the rawId, the key. It can also be used to partition the
 * dictionary based on the hash of the newId, the value, for reverse lookup.
 * <p>
 * Input directory: list of rawid vid pair. Output directory: $outputdir/
 * </p>
 * 
 */
public class SortDictMR {

  private static final Logger LOG = Logger.getLogger(SortDictMR.class);

  /**
   * @param numChunks
   *          number of partitions of the partitioned dictionary.
   * @param hashRawVid
   *          if true, it will partition based on hash(rawId); partition by
   *          hash(newId) otherwise.
   * @param vidparser
   *          {@code FieldParser} for rawId.
   */
  public SortDictMR(int numChunks, boolean hashRawVid, FieldParser vidparser) {
    this.numChunks = numChunks;
    this.hashRawVid = hashRawVid;
    this.vidparser = vidparser;
  }

  /**
   * @param inputpath
   *          the path to a rawId to newId dictionary.
   * @param outputpath
   *          the path of output directory.
   * @throws IOException
   */
  public void run(String inputpath, String outputpath) throws IOException {

    JobConf conf = new JobConf(SortDictMR.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setMapperClass(SortDictMapper.class);
    conf.setReducerClass(SortDictReducer.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setBoolean("hashRawVid", hashRawVid);
    conf.setInt("numChunks", numChunks);
    conf.set("VidParser", vidparser.getClass().getName());

    String outprefix = "vidhashmap";
    for (int i = 0; i < numChunks; i++) {
      MultipleOutputs.addNamedOutput(conf, outprefix + i,
          TextOutputFormat.class, Text.class, Text.class);
    }

    FileInputFormat.setInputPaths(conf, new Path(inputpath));
    FileOutputFormat.setOutputPath(conf, new Path(outputpath));

    LOG.info("========== Job: Partition the map of rawid -> id ===========");
    LOG.info("Input = " + inputpath);
    LOG.info("Output = " + outputpath);
    LOG.info("======================================================");
    if (hashRawVid)
      LOG.info("Partition on rawId.");
    else
      LOG.info("Partition on newId");
    LOG.debug("numChunks = " + numChunks);
    LOG.debug("VidParser = " + vidparser.getClass().getName());
    JobClient.runJob(conf);
    LOG.info("======================= Done ==========================\n");
  }

  private int numChunks;
  private boolean hashRawVid;
  FieldParser vidparser;
}

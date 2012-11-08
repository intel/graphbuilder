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
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;

/**
 * This MapReduce class translate the rawIds in the edge list into normalized
 * newIds using the partitioned edgelist output from SortEdgeMR and partitioned
 * dictionary output from SortDictMR.
 * <p>
 * Input directory: list of edges. Output directory: $outputdir/
 * </p>
 * 
 */
public class TransEdgeMR {

  private static final Logger LOG = Logger.getLogger(TransEdgeMR.class);

  /**
   * @param numChunks
   *          number of partitions in the dictionary.
   * @param dictionaryPath
   *          path of the partitioned dictionary.
   * @param graphparser
   * @param vidparser
   * @param edataparser
   */
  public TransEdgeMR(int numChunks, String dictionaryPath,
      GraphParser graphparser, FieldParser vidparser, FieldParser edataparser) {
    this.numChunks = numChunks;
    this.graphparser = graphparser;
    this.vidparser = vidparser;
    this.edataparser = edataparser;
    this.dictionaryPath = dictionaryPath;
  }

  /**
   * @param inputpath
   *          path of the partitioned edge list
   * @param outputpath
   *          path of the output directory
   * @throws IOException
   */
  public void run(String inputpath, String outputpath) throws IOException {

    JobConf conf = new JobConf(TransEdgeMR.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setMapperClass(TransEdgeMapper.class);
    conf.setReducerClass(TransEdgeReducer.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setInt("numChunks", numChunks);
    conf.set("GraphParser", graphparser.getClass().getName());
    conf.set("VidParser", vidparser.getClass().getName());
    conf.set("EdataParser", edataparser.getClass().getName());

    conf.set("dictionaryPath", dictionaryPath);

    FileInputFormat.setInputPaths(conf, new Path(inputpath));
    FileOutputFormat.setOutputPath(conf, new Path(outputpath));

    LOG.info("============= Job: Normalize Ids in Edges ====================");
    LOG.info("Input = " + inputpath);
    LOG.info("Output = " + outputpath);
    LOG.info("Dictionary = " + dictionaryPath);
    LOG.debug("numChunks = " + numChunks);
    LOG.debug("GraphParser = " + graphparser.getClass().getName());
    LOG.debug("VidParser = " + vidparser.getClass().getName());
    LOG.debug("EdataParser = " + edataparser.getClass().getName());
    LOG.info("===============================================================");

    JobClient.runJob(conf);

    LOG.info("========================= Done ===============================");
  }

  private int numChunks;
  GraphParser graphparser;
  FieldParser vidparser;
  FieldParser edataparser;
  private String dictionaryPath;
}

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
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.io.MultiDirOutputFormat;
import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;

/**
 * This MapReduce class maps a list of unique vertex into 2 parts of output: A
 * dictionary from rawId to newId, and a new vertex data file using newId. The
 * domain of newId is consecutive integers from 0 to |V|-1.
 * <p>
 * Input directory: list of unique vertex data. Output directory:
 * <ul>
 * <li>$outputdir/vidmap for rawid to newid dictionary.</li>
 * <li>$outputdir/vdata for normalized vertex data.</li>
 * </p>
 * 
 */
public class HashIdMR {
  private static final Logger LOG = Logger.getLogger(HashIdMR.class);

  /**
   * Create the MapReduce Job with GraphParser, and vertex FieldParsers.
   * 
   * @param graphparser
   * @param vidparser
   * @param vdataparser
   */
  public HashIdMR(GraphParser graphparser, FieldParser vidparser,
      FieldParser vdataparser) {
    this.graphparser = graphparser;
    this.vidparser = vidparser;
    this.vdataparser = vdataparser;
  }

  /**
   * @param inputpath
   *          the path to a unique vertex list. Each line is parsed into (vid,
   *          data) using {@code vidparser} and {@code vdataparser}.
   * @param outputpath
   *          the path of output directory.
   * @throws IOException
   */
  public void run(String inputpath, String outputpath) throws IOException {
    JobConf conf = new JobConf(HashIdMR.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setMapperClass(HashIdMapper.class);
    conf.setReducerClass(HashIdReducer.class);

    conf.setInputFormat(NLineInputFormat.class);
    conf.setOutputFormat(MultiDirOutputFormat.class);

    conf.setInt("mapred.line.input.format.linespermap", linespermap);
    conf.set("GraphParser", graphparser.getClass().getName());
    conf.set("VidParser", vidparser.getClass().getName());
    conf.set("VdataParser", vdataparser.getClass().getName());

    FileInputFormat.setInputPaths(conf, new Path(inputpath));
    FileOutputFormat.setOutputPath(conf, new Path(outputpath));

    LOG.info("====== Job: Create integer Id maps for vertices ==========");
    LOG.info("Input = " + inputpath);
    LOG.info("Output = " + outputpath);
    LOG.debug("Lines per map = 6000000");
    LOG.debug("GraphParser = " + graphparser.getClass().getName());
    LOG.debug("VidParser = " + vidparser.getClass().getName());
    LOG.debug("VdataParser = " + vdataparser.getClass().getName());
    LOG.info("==========================================================");
    JobClient.runJob(conf);
    LOG.info("=======================Done =====================\n");
  }

  private GraphParser graphparser;
  private FieldParser vidparser;
  private FieldParser vdataparser;
  private static int linespermap = 6000000;
}
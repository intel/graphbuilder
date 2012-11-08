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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.io.MultiDirOutputFormat;

/**
 * The MapRedue class takes from input directory a list of {@code VertexRecrod}
 * and distribute each record to its partitions. The output directory is
 * <p>
 * For example: this vrecord {gvid:"123","mirrors":[0,1,2,4],...} will appear in
 * the partition 0,1,2,4 of the output directory.
 * </p>
 * <p>
 * Input directory contains list of vertex records. Output directory structure:
 * <ul>
 * <li>$outputdir/partition{$i}/vrecord for list of vertex records.</li>
 * <li>$outputdir/partition{$i}/meta for meta info.</li>
 * </ul>
 * </p>
 * 
 */
public class VrecordIngressMR {
  private static final Logger LOG = Logger.getLogger(VrecordIngressMR.class);

  /**
   * Set compress option for the output, by default is false.
   * 
   * @param gzip
   */
  public void useGzip(boolean gzip) {
    this.gzip = gzip;
  }

  public void run(int numProcs, String inputpath, String outputpath)
      throws IOException {

    JobConf conf = new JobConf(VrecordIngressMR.class);
    conf.setJobName("Vrecord Mapreduce");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setMapperClass(VrecordIngressMapper.class);
    conf.setReducerClass(VrecordIngressReducer.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(MultiDirOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(inputpath));
    FileOutputFormat.setOutputPath(conf, new Path(outputpath));

    if (gzip) {
      TextOutputFormat.setCompressOutput(conf, true);
      TextOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    }

    LOG.info("====== Job: Distributed Vertex Records to partitions =========");
    LOG.info("input: " + inputpath);
    LOG.info("output: " + outputpath);
    LOG.info("numProc = " + numProcs);
    LOG.info("gzip = " + Boolean.toString(gzip));
    LOG.info("==============================================================");

    JobClient.runJob(conf);
    LOG.info("==========================Done===============================");
  }

  private boolean gzip = false;
}

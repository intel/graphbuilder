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
package com.intel.hadoop.graphbuilder.preprocess.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;


public class VertexToKVStoreMR {
  private static final Logger LOG = Logger.getLogger(VertexToKVStoreMR.class);

  /**
   * Create the MapReduce Job with GraphParser, and vertex FieldParsers.
   * 
   * @param graphparser
   * @param vidparser
   * @param vdataparser
   */
  public VertexToKVStoreMR(GraphParser graphparser, FieldParser vidparser,
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
  public void run(String inputpath, String tableName) throws IOException {
    JobConf conf = new JobConf(VertexToKVStoreMR.class);

    conf.setMapperClass(VertexToKVStoreMapper.class);
    conf.setNumReduceTasks(0);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(NullOutputFormat.class);

    conf.set("GraphParser", graphparser.getClass().getName());
    conf.set("VidParser", vidparser.getClass().getName());
    conf.set("VdataParser", vdataparser.getClass().getName());

    conf.set("VTable", tableName);
    FileInputFormat.setInputPaths(conf, new Path(inputpath));

    conf.set("GraphParser", graphparser.getClass().getName());
    conf.set("VidParser", vidparser.getClass().getName());
    conf.set("VdataParser", vdataparser.getClass().getName());


    hbaseConf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(hbaseConf);
    if (!admin.tableExists(tableName)) {
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        tableDesc.addFamily(new HColumnDescriptor("vdata"));
        admin.createTable(tableDesc);
        LOG.info("create table " + tableName + " ok.");
    }

    LOG.info("====== Job: Create Vertices Key-Value store ==========");
    LOG.info("Input = " + inputpath);
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

  private Configuration hbaseConf;
}

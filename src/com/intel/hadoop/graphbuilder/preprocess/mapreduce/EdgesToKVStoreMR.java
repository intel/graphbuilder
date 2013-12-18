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
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.PairListType;

/**
 * This job is to write adjacent list of each node to KV store to assist fast queries
 */
public class EdgesToKVStoreMR {
  private static final Logger LOG = Logger.getLogger(EdgesToKVStoreMR.class);
  public static final boolean OUTEDGES = false;
  public static final boolean INEDGES = true;

  /**
   * Create a EdgeTransform Job with {@code inOutEdges} and parsers.
   * 
   * @param inOutEdges
   *          {OUTEDGES, INEDGES} the edge end point to reduce on.
   * @param graphparser
   * @param vidparser
   * @param edataparser
   */
  public EdgesToKVStoreMR(boolean inOutEdges, GraphParser graphparser,
      FieldParser vidparser, FieldParser edataparser) {
    this.inOutEdges = inOutEdges;
    this.graphparser = graphparser;
    this.vidparser = vidparser;
    this.edataparser = edataparser;
    conf = new JobConf(EdgesToKVStoreMR.class);
  }

  /**
   * Set the intermediate key value class.
   * 
   * @param keyClass
   * @param valClass
   */
  public void setKeyValueClass(Class keyClass, Class valClass) {
    try {
      this.mapkeytype = keyClass.newInstance();
      this.mapvaltype = (PairListType) valClass.newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  /**
   * Set the user defined options.
   * 
   * @param userOpts
   *          a Map of option key value pairs.
   */
  public void setUserOptions(HashMap<String, String> userOpts) {
    Set<String> s = userOpts.keySet();
    for (String key : s)
      conf.set(key, userOpts.get(key.toString()));
  }

  /**
   * @return JobConf of the current job.
   */
  public JobConf getConf() {
    return conf;
  }

  public void run(String inputpath, String tableName, String numEdges) throws IOException {

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapOutputKeyClass(mapkeytype.getClass());
    conf.setMapOutputValueClass(mapvaltype.getClass());

    conf.setMapperClass(EdgesToKVStoreMapper.class);
    conf.setCombinerClass(EdgesToKVStoreCombiner.class);
    conf.setReducerClass(EdgesToKVStoreReducer.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setBoolean("InOutEdges", inOutEdges);
    conf.set("GraphParser", graphparser.getClass().getName());
    conf.set("VidParser", vidparser.getClass().getName());
    conf.set("EdataParser", edataparser.getClass().getName());
    conf.setInt("NumEdges", Integer.parseInt(numEdges));
    conf.set("EdgeTable", tableName);

    FileInputFormat.setInputPaths(conf, new Path(inputpath));
    // so ugly, should get rid of output filei. we are not using any output file
    FileOutputFormat.setOutputPath(conf, new Path(inputpath + "edgestokvstoretemp"));

    hbaseConf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(hbaseConf);
    if (admin.tableExists(tableName)) {
       LOG.info("table already exists!");
    }  else {
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        tableDesc.addFamily(new HColumnDescriptor("outEdges"));
        tableDesc.addFamily(new HColumnDescriptor("inEdges"));
        admin.createTable(tableDesc);
        LOG.info("create table " + tableName + " ok.");
    }
    LOG.info("============== Job: Data Transformation on Edges ==========");
    LOG.info("Input = " + inputpath);
    LOG.info("NumEdges = " + Integer.parseInt(numEdges));
    //LOG.info("Output = " + outputpath);
    if (inOutEdges == OUTEDGES)
      LOG.info("Get outedge list");
    else
      LOG.info("Get inedge list");

    LOG.info("===========================================================");
    JobClient.runJob(conf);

    try {
      FileSystem fs = FileSystem.get(conf);
      fs.delete(new Path(inputpath + "edgestokvstoretemp"), true);
    } catch (IOException e) {
      e.printStackTrace();
    }

    LOG.info("======================== Done ============================\n");
  }


  private Object mapkeytype;
  private PairListType mapvaltype;

  private GraphParser graphparser;
  private FieldParser vidparser;
  private FieldParser edataparser;
  private boolean inOutEdges;
  private JobConf conf;
  private Configuration hbaseConf;
}

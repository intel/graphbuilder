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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.intel.hadoop.graphbuilder.graph.Vertex;
import com.intel.hadoop.graphbuilder.parser.GraphParser;
import com.intel.hadoop.graphbuilder.parser.FieldParser;

public class VertexToKVStoreMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, Text> {

  private static final Logger LOG = Logger.getLogger(VertexToKVStoreMapper.class);

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    try {
      this.graphparser = (GraphParser) Class.forName(job.get("GraphParser"))
          .newInstance();
      this.vidparser = (FieldParser) Class.forName(job.get("VidParser"))
          .newInstance();
      this.vdataparser = (FieldParser) Class.forName(job.get("VdataParser"))
          .newInstance();

      conf =  HBaseConfiguration.create();
      String tableName = job.get("VTable");  
      htable = new HTable(conf, tableName);
      htable.setAutoFlush(false);
      htable.setWriteBufferSize(1024 * 1024 * 100);
      conf.setInt("hbase.regionserver.optionallogflushinterval", 5000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void close() throws IOException {
    htable.flushCommits();
    htable.close();
  }

  @Override
  public void map(LongWritable key, Text val,
      OutputCollector<Text, Text> out, Reporter arg3) throws IOException {

     Vertex v = graphparser.parseVertex(val.toString(), vidparser,
            vdataparser);
    
     String hKey = new String(v.vid().toString());;
     String hValue = new String(v.vdata().toString());

     Put put =  new Put(Bytes.toBytes(hKey));
     put.add(Bytes.toBytes("vdata"), Bytes.toBytes("vdata"),
              Bytes.toBytes(hValue));

    htable.put(put);
     
  }

  private GraphParser graphparser;
  private FieldParser vidparser;
  private FieldParser vdataparser;
  Configuration conf;
  HTable htable;
}

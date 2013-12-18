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
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.PairListType;
import com.intel.hadoop.graphbuilder.util.Pair;

public class EdgesToKVStoreReducer<VidType extends WritableComparable<VidType>>
    extends MapReduceBase implements Reducer<VidType, PairListType, Text, Text>
 {
private static final Logger LOG = Logger.getLogger(EdgesToKVStoreReducer.class);
   @Override
  public void configure(JobConf job) {
    super.configure(job);
    this.inOutEdges = job.getBoolean("InOutEdges",
        EdgesToKVStoreMR.OUTEDGES); 
    try {
        conf =  HBaseConfiguration.create();
        String tableName = job.get("EdgeTable");
        htable = new HTable(conf, tableName);
       
        htable.setAutoFlush(false);
        htable.setWriteBufferSize(1024 * 1024 * 100);
        threshold = 200;
        numEdges = job.getInt("NumEdges",0);

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
  public void reduce(VidType key, Iterator<PairListType> values,
      OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
    ArrayList<VidType> vids = new ArrayList<VidType>();
    ArrayList<Writable> data = new ArrayList<Writable>();

    while (values.hasNext()) {
      PairListType list = values.next();
      Iterator<Pair<VidType, Writable>> iter = list.iterator();
      while (iter.hasNext()) {
        Pair<VidType, Writable> pair = iter.next();
        vids.add(pair.getL());
        data.add(pair.getR());
      }
    }

    hKey = new String(key.toString());
    hValue = new String();
    int size = vids.size();

    if (numEdges != 0 && size > numEdges) 
        size = numEdges;

    int parts = size / threshold;

    // edge list; vid, vid edgevalue| vid edgevalue|....|
    for (int i = 1; i <= size; i++) {
      hValue +=  vids.get(i-1).toString() + "\t" + data.get(i-1).toString() + "|";
      
      if (i%threshold==0) {
        Put put;
        if (i==threshold)
          put = new Put(Bytes.toBytes(hKey));  
        else 
          put = new Put(Bytes.toBytes(hKey+"-p"+Integer.toString(i/threshold-1)));
    
        // disable writeWAL for improving performance
        put.setWriteToWAL(false);     
        if (inOutEdges == EdgesToKVStoreMR.OUTEDGES) {
          put.add(Bytes.toBytes("outEdges"), Bytes.toBytes("outEdges"),
              Bytes.toBytes(hValue));
        } 
        else {
          put.add(Bytes.toBytes("inEdges"), Bytes.toBytes("inEdges"),
            Bytes.toBytes(hValue));
        } 
      
        htable.put(put);
        hValue = new String();
      }
   }      

    if (size % threshold !=0) {
      int part = size / threshold;

      Put put;
      if (part==0) 
        put = new Put(Bytes.toBytes(hKey));
      else
        put = new Put(Bytes.toBytes(hKey+"-p"+Integer.toString(part)));

      put.setWriteToWAL(false);
      if (inOutEdges == EdgesToKVStoreMR.OUTEDGES) {
          put.add(Bytes.toBytes("outEdges"), Bytes.toBytes("outEdges"), 
            Bytes.toBytes(hValue));
      }
      else {
          put.add(Bytes.toBytes("inEdges"), Bytes.toBytes("inEdges"),
            Bytes.toBytes(hValue));  
      } 
      htable.put(put);  
    }

  }

  String hKey;
  String hValue;
  private boolean inOutEdges;
  private int threshold;
  private int numEdges;
  Configuration conf;
  HTable htable;
  
}

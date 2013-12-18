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
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.intel.hadoop.graphbuilder.graph.Edge;
import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;
import com.intel.hadoop.graphbuilder.preprocess.functional.Functional;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.PairListType;
import com.intel.hadoop.graphbuilder.util.Pair;

/**
 * The Reduce functional computes a sufficient statistics on a list of edges,
 * and the Apply functional applies it back to each edge in the list.
 * 
 * 
 * @param <VidType>
 */
public class EdgeTransformReducer<VidType extends WritableComparable<VidType>>
    extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text> {
  @Override
  public void configure(JobConf job) {
    super.configure(job);
    this.reduceEndPoint = job.getBoolean("reduceEndPoint",
        EdgeTransformMR.SOURCE);
    try {
      this.reduceFunc = (Functional) Class.forName(job.get("ReduceFunc"))
          .newInstance();
      this.applyFunc = (Functional) Class.forName(job.get("ApplyFunc"))
          .newInstance();
      this.reduceFunc.configure(job);
      this.applyFunc.configure(job);
      this.graphparser = (GraphParser) Class.forName(job.get("GraphParser"))
          .newInstance();
      this.vidparser = (FieldParser) Class.forName(job.get("VidParser"))
          .newInstance();
      this.edataparser = (FieldParser) Class.forName(job.get("EdataParser"))
          .newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    textKey = new Text();
    textValue = new Text();
  }

  @Override
  public void reduce(IntWritable key, Iterator<Text> values,
      OutputCollector<Text, Text> out, Reporter reporter) throws IOException {

    adjlist = new HashMap<VidType, ArrayList<VidType>>();
    edata = new HashMap<VidType, ArrayList<Writable>>();

    while (values.hasNext()) {
      String line = values.next().toString();
      StringTokenizer st = new StringTokenizer(line, "|");

      while (st.hasMoreTokens()) {
        String subline = st.nextToken();
        Edge e = graphparser.parseEdge(subline, vidparser, edataparser);
        
        if (adjlist.containsKey(e.source())) {
            adjlist.get(e.source()).add((VidType)e.target());
            edata.get(e.source()).add(e.EdgeData());
        } else {
            ArrayList<VidType> vidlist = new ArrayList<VidType>();
            vidlist.add((VidType)e.target());
            ArrayList<Writable> elist = new ArrayList<Writable>();
            elist.add((Writable)e.EdgeData());
            adjlist.put((VidType)e.source(), vidlist);
            edata.put((VidType)e.source(), elist);
          }
       }
    }

    // Reduce
    Iterator<Entry<VidType, ArrayList<VidType>>> viter = adjlist.entrySet().iterator();
    Iterator<Entry<VidType, ArrayList<Writable>>> diter = edata.entrySet().iterator();
    
    while (viter.hasNext() && diter.hasNext()) {
      Entry<VidType, ArrayList<VidType>> adjlist_e = viter.next();
      Entry<VidType, ArrayList<Writable>> edata_e = diter.next();
      ArrayList<VidType> target_list = adjlist_e.getValue();
      ArrayList<Writable> edata_list = edata_e.getValue();
    
      Writable r = reduceFunc.base();
      
      for (int i = 0; i < target_list.size(); i++)
        r = reduceFunc.reduce(edata_list.get(i), r);

      for (int i = 0; i < target_list.size(); i++) {
        Writable res = applyFunc.reduce(edata_list.get(i), r);
    
        if (reduceEndPoint == EdgeTransformMR.SOURCE) {
          
          textKey.set(adjlist_e.getKey().toString());
          textValue.set(target_list.get(i).toString() + "\t" + res.toString());
          out.collect(textKey, textValue);

        } else {
          textKey.set(target_list.get(i).toString());
          textValue.set(adjlist_e.getKey().toString()+ "\t" + res.toString());
          out.collect(textKey, textValue);
        }
      }
    }
  }

  private Text textKey;
  private Text textValue;
  private HashMap<VidType, ArrayList<VidType>> adjlist;
  private HashMap<VidType, ArrayList<Writable>> edata;

  private GraphParser graphparser;
  private FieldParser edataparser;
  private FieldParser vidparser;
  private Functional reduceFunc;
  private Functional applyFunc;
  private boolean reduceEndPoint;
}

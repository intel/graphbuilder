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
    extends MapReduceBase implements Reducer<VidType, PairListType, Text, Text> {
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
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
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

    // Reduce
    Writable r = reduceFunc.base();
    for (int i = 1; i < data.size(); i++)
      r = reduceFunc.reduce(data.get(i), r);

    // Apply
    for (int i = 0; i < data.size(); i++) {
      Writable res = applyFunc.reduce(data.get(i), r);
      // Output
      if (reduceEndPoint == EdgeTransformMR.SOURCE)
        out.collect(new Text(key.toString()), new Text(vids.get(i).toString()
            + "\t" + res.toString()));
      else
        out.collect(new Text(vids.get(i).toString()), new Text(key.toString()
            + "\t" + res.toString()));
    }
  }

  private Functional reduceFunc;
  private Functional applyFunc;
  private boolean reduceEndPoint;
}

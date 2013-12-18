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
import java.util.Iterator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.PairListType;


public class EdgesToKVStoreCombiner<VidType extends WritableComparable<VidType>>
    extends MapReduceBase implements
    Reducer<VidType, PairListType, VidType, PairListType> {

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    this.valClass = job.getMapOutputValueClass();
  }

  @Override
  public void reduce(VidType key, Iterator<PairListType> values,
      OutputCollector<VidType, PairListType> out, Reporter reporter)
      throws IOException {
    PairListType ret;
    try {
      ret = (PairListType) valClass.newInstance();
      while (values.hasNext()) {
        ret.append(values.next());
      }
      out.collect(key, ret);
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  protected Class valClass;
}

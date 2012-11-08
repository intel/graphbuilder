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
package com.intel.hadoop.graphbuilder.partition.mapreduce.edge;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.CombinedEdgeValueType;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.IngressKeyType;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.IngressValueType;

/**
 * Combines intermediate IngressValues.
 * 
 * @param <KeyType>
 * @param <ValType>
 */
public class EdgeIngressCombiner<KeyType extends IngressKeyType, ValType extends IngressValueType>
    extends MapReduceBase implements
    Reducer<KeyType, ValType, KeyType, ValType> {

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    this.valClass = job.getMapOutputValueClass();
  }

  @Override
  public void reduce(KeyType key, Iterator<ValType> iter,
      OutputCollector<KeyType, ValType> out, Reporter reporter)
      throws IOException {
    if (!iter.hasNext()) {
      return;
    }

    ValType ret;
    try {
      ret = (ValType) valClass.newInstance();

      if (key.flag() == IngressKeyType.EDGEKEY) {
        CombinedEdgeValueType eval = ret.edgeValue();
        ret.reduce(IngressKeyType.EDGEKEY, iter);
        out.collect(key, ret);
      } else if (key.flag() == IngressKeyType.VERTEXKEY) {
        ret.reduce(IngressKeyType.VERTEXKEY, iter);
        out.collect(key, ret);
      }
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }

  }

  protected Class valClass;
}

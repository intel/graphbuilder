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
package com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue;

import java.util.ArrayList;
import java.util.Iterator;

import com.intel.hadoop.graphbuilder.util.Pair;

/**
 * Helper class for iterating a list of pairs.
 * 
 * @param <T1>
 * @param <T2>
 */
public class PairListIterator<T1, T2> implements Iterator<Pair<T1, T2>> {

  public PairListIterator() {
  }

  public PairListIterator(ArrayList<T1> l1, ArrayList<T2> l2) throws Exception {
    list1 = l1;
    list2 = l2;
    if (list1.size() != list2.size())
      throw new Exception("List size not equal");
  }

  @Override
  public boolean hasNext() {
    return list1 == null ? false : cur < list1.size();
  }

  @Override
  public Pair<T1, T2> next() {
    int i = cur;
    cur++;
    return new Pair(list1.get(i), list2.get(i));
  }

  @Override
  public void remove() {
    list1.remove(cur);
    list2.remove(cur);
  }

  private int cur;
  private ArrayList<T1> list1;
  private ArrayList<T2> list2;
}

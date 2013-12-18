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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;

import com.intel.hadoop.graphbuilder.util.Pair;

/**
 * A general abstract class represents a list of pairs. Used as the intermediate
 * map output value type in EdgeTransformMR for holding a list of
 * {@code VidType, EdgeData} pairs.
 * 
 * @param <T1>
 * @param <T2>
 */
public abstract class PairListType<T1 extends Writable, T2 extends Writable>
    implements Writable {

  /**
   * Creates an empty value.
   */
  public PairListType() {
    this.llist = new ArrayList();
    this.rlist = new ArrayList();
  }

  public abstract T1 createLValue();

  public abstract T2 createRValue();

  /**
   * @return number of pairs in the list.
   */
  public int size() {
    return llist().size();
  }

  /**
   * Add a pair into the list.
   * 
   * @param e1
   * @param e2
   */
  public void add(T1 e1, T2 e2) {
    llist.add(e1);
    rlist.add(e2);
  }

  /**
   * Initialize the list with a pair.
   * 
   * @param e1
   * @param e2
   */
  public void init(T1 e1, T2 e2) {
    llist.clear();
    rlist.clear();
    llist.add(e1);
    rlist.add(e2);
  }

  public List<T1> llist() {
    return llist;
  }

  public List<T2> rlist() {
    return rlist;
  }

  /**
   * Append the other list to the end.
   * 
   * @param other
   */
  public void append(PairListType<T1, T2> other) {
    for (T1 ele : other.llist()) {
      llist.add(ele);
    }
    for (T2 ele : other.rlist()) {
      rlist.add(ele);
    }
  }

  /**
   * @return iterator at the begining of the list.
   */
  public Iterator<Pair<T1, T2>> iterator() {
    try {
      return new PairListIterator<T1, T2>(llist, rlist);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    llist.clear();
    rlist.clear();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      T1 e1 = createLValue();
      e1.readFields(in);
      T2 e2 = createRValue();
      e2.readFields(in);
      llist.add(e1);
      rlist.add(e2);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(size());
    for (int i = 0; i < llist.size(); i++) {
      llist.get(i).write(out);
      rlist.get(i).write(out);
    }
  }

  protected ArrayList<T1> llist;
  protected ArrayList<T2> rlist;
}

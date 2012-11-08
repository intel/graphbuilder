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
package com.intel.hadoop.graphbuilder.graph.glgraph;

import java.util.ArrayList;
import java.util.List;

/**
 * Stores a list of edges as 3 separate arrays of source id, target id and edge
 * data respectively. This class is used as a temporary storage for edges in the
 * {@code GLGraph} before finalization.
 * 
 * @see GLGraph#addEdge(Object, Object, Object)
 * @see GLGraph#finalize()
 * @param <EdgeData>
 */
public class EdgeListStorage<EdgeData> {
  /**
   * Default constructor of an empty EdgeListStorage.
   */
  public EdgeListStorage() {
    edata = new ArrayList<EdgeData>();
    sources = new ArrayList<Integer>();
    targets = new ArrayList<Integer>();
  }

  /**
   * Creates an empty EdgeListStorage with expected size.
   * 
   * @param size
   */
  public EdgeListStorage(int size) {
    edata = new ArrayList<EdgeData>(size);
    sources = new ArrayList<Integer>(size);
    targets = new ArrayList<Integer>(size);
  }

  /**
   * Reserves the space for n edges.
   * 
   * @param n
   *          the number of edges to be added.
   */
  public void reserve(int n) {
    edata.ensureCapacity(n);
    sources.ensureCapacity(n);
    targets.ensureCapacity(n);
  }

  /**
   * Add an edge with source, target and edata to the storage. Thread safe.
   * 
   * @param source
   * @param target
   * @param data
   */
  public void addEdge(Integer source, Integer target, EdgeData data) {
    synchronized (this) {
      sources.add(source);
      targets.add(target);
      edata.add(data);
    }
  }

  /**
   * Add a list of edges. Thread safe.
   * 
   * @param sourceList
   * @param targetList
   * @param dataList
   * @throws Exception
   */
  public void addEdges(List<Integer> sourceList, List<Integer> targetList,
      List<EdgeData> dataList) throws Exception {
    synchronized (this) {
      if (!(sourceList.size() == targetList.size() && sourceList.size() == dataList
          .size())) {
        throw new Exception(
            "Attempt to add edge list whose source, target and edata"
                + " have different length.");
      }
      sources.addAll(sourceList);
      targets.addAll(targetList);
      edata.addAll(dataList);
    }
  }

  /**
   * @return the number of edges in the storage.
   */
  public int size() {
    return sources.size();
  }

  /**
   * Removing all edges from the storage.
   */
  public void clear() {
    edata.clear();
    sources.clear();
    targets.clear();
  }

  /**
   * Inplace shuffle the edges in the storage by a permutation array.
   * 
   * @permute A permutation array of the same size as the storage.
   * @throws Exception
   */
  public void inplace_shuffle(List<Integer> permute) throws Exception {
    if (permute.size() != edata.size()) {
      throw new Exception("Attempt to shuffle"
          + "the edgelist with permutation array of different size.");
    }
    for (int i = 0; i < permute.size(); ++i) {
      if (i != permute.get(i)) {
        int sourceSwap = sources.get(i);
        int targetSwap = targets.get(i);
        EdgeData edataSwap = edata.get(i);
        int j = i;
        while (j != permute.get(j)) {
          int next = permute.get(j);
          if (next != i) {
            sources.set(j, sources.get(next));
            targets.set(j, targets.get(next));
            edata.set(j, edata.get(next));
            permute.set(j, j);
            j = next;
          } else {
            sources.set(j, sourceSwap);
            targets.set(j, targetSwap);
            edata.set(j, edataSwap);
            permute.set(j, j);
            break;
          }
        }
      }
    }
  }

  /**
   * A list of edge data.
   */
  public ArrayList<EdgeData> edata;
  /**
   * A list of source vertex ids.
   */
  public ArrayList<Integer> sources;
  /**
   * A list of target vertex ids.
   */
  public ArrayList<Integer> targets;
}

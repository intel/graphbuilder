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
package com.intel.hadoop.graphbuilder.partition.strategy;

import com.intel.hadoop.graphbuilder.util.HashUtil;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.Math;
import java.util.Collections;
/**
 * The processors in the cluster are assigned to elements of a nxn grid
 * where n = sqrt(processors)is divided into a constraint grid. The edges
 * are assigned randomly (uniformly) among the processors given that
 * they belong to the constraint grid. For example.
 * If the grid is
 *               1 | 2 | 3
 *               4 | 5 | 6
 *               7 | 8 | 9
 * a vertex mapped to node 2, will have its constrained grid = {1,2,3,5,8} 
 *
 * @param <VidType>
 */
public class ConstrainedRandomIngress<VidType> implements Ingress<VidType> {

  /**
   * Default constructor with numProcs set.
   * 
   * @param numProcs
   */
  public ConstrainedRandomIngress(int numProcs) {
    this.numProcs = numProcs;
    constraintGraph = new HashMap<Integer, ArrayList<Integer>>();
    joinShards = new ArrayList<Integer>();
    makeGridConstrain();
  }

  private void makeGridConstrain() {
    int nCols;
    int nRows;

    nCols = nRows = (int)Math.sqrt(numProcs);
    
    for (int i=0; i<numProcs; i++) {
      ArrayList<Integer> adjList = new ArrayList<Integer>();
      adjList.add(new Integer(i));
      
      int rowBegin=(i/nCols) * nCols;
      int j;
      for (j=rowBegin; j<(rowBegin + nCols); j++)
        if (i!=j) adjList.add(new Integer(j));
      
      for (j=i%nCols; j<numProcs; j+=nCols)
        if (i!=j) adjList.add(new Integer(j));

      Collections.sort(adjList);
      Integer key = new Integer(i);
      if (!constraintGraph.containsKey(key))
        constraintGraph.put(key, adjList);
    }
  }
  private ArrayList<Integer> getJoinShards(int srcMaster, int targetMaster)
  {
    ArrayList<Integer> srcShards = constraintGraph.get(new Integer(srcMaster));
    ArrayList<Integer> targetShards = constraintGraph.get(new Integer(targetMaster));
    joinShards.clear();

    int i=0;
    int j=0;
    while(i<srcShards.size() && j<targetShards.size()) {
      if(srcShards.get(i).intValue()==targetShards.get(j).intValue()) {
        joinShards.add(srcShards.get(i));
        i++;
        j++;
      }
      else if (srcShards.get(i).intValue() < targetShards.get(j).intValue()) {
        i++;
      } else {
        j++;
      }
    }
    return joinShards;
  }

  @Override
  public short computePid(VidType source, VidType target) {
  
    int srcMaster = Math.abs(source.hashCode()) % numProcs;
    int targetMaster = Math.abs(target.hashCode()) % numProcs;

    ArrayList<Integer> candidates = getJoinShards(srcMaster, targetMaster);

    short index = (short) (HashUtil.hashpair(source, target) % candidates.size());
    if (index < 0)
      index = (short) (index + candidates.size());
    
    return (short)candidates.get(index).intValue();
  }

  @Override
  public int numProcs() {
    return numProcs;
  }

  private ArrayList<Integer> joinShards;
  private HashMap<Integer, ArrayList<Integer>> constraintGraph;
  private int numProcs;
}

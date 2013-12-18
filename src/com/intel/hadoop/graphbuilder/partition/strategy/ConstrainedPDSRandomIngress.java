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
 * Random assigns a partition id for each edge.
 * The algorithm is influenced by the constrained based partitioning of graphs
 * using Perfect Difference Set (PDS) method. In constrained partitioning, vertices
 * are hashed to shard S based on the vid. Mirrors of the vertex can then be
 * only mapped to a set of shards based on a constrained mapping. In this
 * strategy, the constrained set is determined using PDS. Refer to distributed
 * Graphlab 2.1 sharding_constraint code and documentation for further details
 * @param <VidType>
 */
public class ConstrainedPDSRandomIngress<VidType> implements Ingress<VidType> {

  /**
   * Default constructor with numProcs set.
   * 
   * @param numProcs
   */
  public ConstrainedPDSRandomIngress(int numProcs) {
    this.numProcs = numProcs;
    constraintGraph = new HashMap<Integer, ArrayList<Integer>>();
    joinShards = new ArrayList<Integer>();
    makePdsConstrain();
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


  /**
   * Reuse the implementation in GraphLab 2.1
   */

  private boolean test_seq(int a, int b, int c, int p, ArrayList<Integer> pds) {
  
    ArrayList<Integer> seq = new ArrayList<Integer> ();
    int pdslength = p*p + p + 1;
    seq.add(new Integer(0));
    seq.add(new Integer(0));
    seq.add(new Integer(1));
    int ctr = 2;
   
    for (int i = 3; i < (pdslength + 3); ++i) { 
        int temp = a * seq.get(i-1).intValue() + b * seq.get(i-2).intValue()
                + c * seq.get(i-3).intValue();
        seq.add(new Integer(temp %p));
        if (seq.get(i).intValue()==0)
            ctr++;
        if (i < pdslength && ctr > p + 1) return false;
    }
    if (seq.get(pdslength).intValue() == 0 && seq.get(pdslength+1).intValue() == 0) {
        for (int i = 0; i < pdslength; ++i) {
            if (seq.get(i).intValue() == 0) {
                pds.add(new Integer(i));                
            }
        }
        if (pds.size() != p + 1) {
            pds.clear();
            return false;
        } 
        return true;
    }
    else {
        return false;
    }
  }

  /**
   * Reuse the implementation in GraphLab 2.1
   */

  private ArrayList<Integer> findPds (int prime)
  {
    ArrayList<Integer> pds = new ArrayList<Integer>();

    for (int a = 0; a < prime; ++a) {
        for (int b = 0; b < prime; ++b) {
            if (b == 0 && a == 0) continue;
            for (int c = 1; c < prime; ++c) {
                if (test_seq(a,b,c,prime,pds)) {
                    return pds;
                }
            }
        }
    }

    return pds;

  } 
  private void makePdsConstrain() {
    
    ArrayList<Integer> pds;

    int prime = (int)Math.floor(Math.sqrt(numProcs-1));

    if (numProcs != (prime*prime + prime + 1)) {
        // if it does not meet pds requirement, back to grid based constraint
        makeGridConstrain();
    } else {

        pds = findPds(prime);        
        for (int i=0; i< numProcs; i++) {
            ArrayList<Integer> adjList = new ArrayList<Integer>();
            for (int j=0; j<pds.size(); j++) {
                adjList.add(new Integer((pds.get(j).intValue()+i)%numProcs));                    
            }
            Collections.sort(adjList);
            Integer key = new Integer(i);
            if (!constraintGraph.containsKey(key))
                constraintGraph.put(key, adjList);

        }
    } 

  }
  private ArrayList<Integer> getJoinShards(int srcMaster, int targetMaster)
  {
    ArrayList<Integer> srcShards = constraintGraph.get(srcMaster);
    ArrayList<Integer> targetShards = constraintGraph.get(targetMaster);
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
  
    int srcMaster = source.hashCode() % numProcs;
    int targetMaster = target.hashCode() % numProcs;

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

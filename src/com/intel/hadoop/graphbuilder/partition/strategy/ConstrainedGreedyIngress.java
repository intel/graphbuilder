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

import java.lang.Math;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import org.apache.log4j.Logger;
/**
 * Greedy assigns the partition id to minimize the total size of vertex mirrors.
 * This class keeps track of the edges it has seen, and assign the partitions to
 * new edges such that the increase of total vertex mirror size is minimized
 * while the balance among partitions is also maintained.
 *
 * The processors in the cluster are assigned to elements of a nxn grid
 * where n = sqrt(processors)is divided into a constraint grid. The edges
 * are assigned following the greedy method among the processors given that
 * they belong to the constraint grid. For example.
 * If the grid is
 *               1 | 2 | 3
 *               4 | 5 | 6
 *               7 | 8 | 9
 * a vertex mapped to node 2, will have its constrained grid = {1,2,3,5,8}
 *
 * @param <VidType>
 */
public class ConstrainedGreedyIngress<VidType> implements Ingress<VidType> {
 private static final Logger LOG = Logger.getLogger(ConstrainedGreedyIngress.class);
  public ConstrainedGreedyIngress(int numProcs) {
    this.numProcs = numProcs;
    vertexPresence = new HashMap<VidType, BitSet>();
    procLoad = new ArrayList<Integer>(Collections.nCopies(numProcs, 0));
    useHash = true;
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

  @Override
  public short computePid(VidType source, VidType target) {
    short pid = getPid(source, target);
    setVertexPresence(source, pid);
    setVertexPresence(target, pid);
    return pid;
  }

  @Override
  public int numProcs() {
    return numProcs;
  }

  /**
   * Update the tracking table by adding the new assignment of pid to vid.
   * 
   * @param vid
   * @param pid
   */
  private void setVertexPresence(VidType vid, short pid) {
    if (vertexPresence.containsKey(vid)) {
      BitSet bitset = vertexPresence.get(vid);
      if (!bitset.get(pid)) {
        bitset.set(pid);
      }
    } else {
      BitSet bitset = new BitSet(numProcs);
      bitset.set(pid);
      vertexPresence.put(vid, bitset);
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
  /**
   * Computes the "optimal" assignment of the edge by choosing the partition
   * such that the vertex mirror size is the smallest. Break ties by choosing
   * the assignment that keeps the best load balance among all partitions.
   * 
   * @param source
   * @param target
   * @return
   */
  private short getPid(VidType source, VidType target) {
    double epsilon = this.threshold;
    ArrayList<Double> scores = new ArrayList<Double>(Collections.nCopies(
        numProcs, 0.0));
    int minEdges = java.util.Collections.min(procLoad);
    int maxEdges = java.util.Collections.max(procLoad);
    BitSet sourceTable = (vertexPresence.containsKey(source)) ? vertexPresence
        .get(source) : new BitSet(numProcs);
    BitSet targetTable = (vertexPresence.containsKey(target)) ? vertexPresence
        .get(target) : new BitSet(numProcs);

    int srcMaster = Math.abs(source.hashCode()) % numProcs;
    int targetMaster = Math.abs(target.hashCode()) % numProcs;

    ArrayList<Integer> candidates = getJoinShards(srcMaster, targetMaster);
    
    int j=0;
    for (int i = 0; i < candidates.size(); i++) {
      j = candidates.get(i).intValue();
      double bal = (double) (maxEdges - procLoad.get(j))
          / (maxEdges - minEdges + epsilon);
      int sourceScore = (sourceTable.get(j) || (useHash && source.hashCode()
          % numProcs == j)) ? 1 : 0;
      int targetScore = (targetTable.get(j) || (useHash && target.hashCode()
          % numProcs == j)) ? 1 : 0;
      scores.set(j, bal + sourceScore + targetScore);
    }
    double maxScore = java.util.Collections.max(scores);
    ArrayList<Short> bestProcs = new ArrayList<Short>();
    for (int k = 0; k < candidates.size(); k++) {
      int l = candidates.get(k).intValue();
      if (Math.abs(scores.get(l) - maxScore) < 1e-5) {
        bestProcs.add((short) l);
      }
    }

    Random generator = new Random();
    short pid = bestProcs.get(generator.nextInt(bestProcs.size()));
    procLoad.set(pid, procLoad.get(pid) + 1);
    return pid;
  }

  private int numProcs;
  /**
   * A threshold (0,1) to control the importance of balance. The larger
   * threshold, the less significant of the balance is.
   */
  private double threshold = 0.01;
  /** An optimization that pre-assign first seen vertex with a hash function. */
  private boolean useHash;
  private HashMap<VidType, BitSet> vertexPresence;
  private HashMap<Integer, ArrayList<Integer>> constraintGraph; 
  private ArrayList<Integer> joinShards;
  private ArrayList<Integer> procLoad;
}

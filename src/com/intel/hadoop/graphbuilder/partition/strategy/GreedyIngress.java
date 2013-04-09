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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

/**
 * Greedy assigns the partition id to minimize the total size of vertex mirrors.
 * This class keeps track of the edges it has seen, and assign the partitions to
 * new edges such that the increase of total vertex mirror size is minimized
 * while the balance among partitions is also maintained.
 * 
 * @param <VidType>
 */
public class GreedyIngress<VidType> implements Ingress<VidType> {
  public GreedyIngress(int numProcs) {
    this.numProcs = numProcs;
    vertexPresence = new HashMap<VidType, BitSet>();
    procLoad = new ArrayList<Integer>(Collections.nCopies(numProcs, 0));
    useHash = true;
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

    for (int j = 0; j < numProcs; j++) {
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
    for (int j = 0; j < numProcs; j++) {
      if (Math.abs(scores.get(j) - maxScore) < 1e-5) {
        bestProcs.add((short) j);
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
  private ArrayList<Integer> procLoad;
}

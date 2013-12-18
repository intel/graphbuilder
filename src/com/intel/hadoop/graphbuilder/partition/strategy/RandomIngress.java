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

/**
 * Random assigns a partition id for each edge.
 * All nodes in the cluster gets equal share of edges
 * 
 * @param <VidType>
 */
public class RandomIngress<VidType> implements Ingress<VidType> {

  /**
   * Default constructor with numProcs set.
   * 
   * @param numProcs
   */
  public RandomIngress(int numProcs) {
    this.numProcs = numProcs;
  }

  @Override
  public short computePid(VidType source, VidType target) {
    short pid = (short) (HashUtil.hashpair(source, target) % numProcs);
    if (pid < 0)
      pid = (short) (pid + numProcs);
    return pid;
  }

  @Override
  public int numProcs() {
    return numProcs;
  }

  private int numProcs;
}

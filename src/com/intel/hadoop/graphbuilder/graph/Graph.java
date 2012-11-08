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
package com.intel.hadoop.graphbuilder.graph;

import java.util.List;

/**
 * A distributed graph partition consists of 3 parts: a partition id, a
 * collection of local edges, and a collection of {@code VertexRecord}s.
 * 
 * @param <VidType>
 * @param <VertexData>
 * @param <EdgeData>
 */
public interface Graph<VidType, VertexData, EdgeData> {

  /**
   * @return partition id of this graph.
   */
  int pid();

  /**
   * @param pid
   *          partition id of this graph.
   */
  void setPid(int pid);

  /**
   * @param numEdges
   *          expected number of edges in the graph.
   */
  void reserveEdgeSpace(int numEdges);

  /**
   * @param numVertices
   *          expected number of vertices in the graph.
   */
  void reserveVertexSpace(int numVertices);

  /**
   * @return number of vertices in the graph.
   */
  int numVertices();

  /**
   * @return number of edges in the graph.
   */
  int numEdges();

  /**
   * Batch add a collection of edges to the graph.
   * 
   * @param sources
   *          List of source vertex ids.
   * @param targets
   *          List of target vertex ids.
   * @param edata
   *          List of edge data.
   * @throws Exception
   *           when any of the input is null or sources, targets and edata are
   *           of different length
   */
  void addEdges(List<VidType> sources, List<VidType> targets,
      List<EdgeData> edata) throws Exception;

  /**
   * Add a single edge to the graph.
   * 
   * @param source
   *          the source vertex id of the edge to be added.
   * @param target
   *          the target vertex id of the edge to be added.
   * @param edata
   *          the edge data of the edge to be added.
   */
  void addEdge(VidType source, VidType target, EdgeData edata);

  /**
   * Add a vertex record to the graph.
   * 
   * @param vrecord
   *          the {@code VertexRecord} object to be added.
   * @see VertexRecord
   */
  void addVertexRecord(VertexRecord<VidType, VertexData> vrecord);

  /**
   * This method shall be called to ensure that the graph is in its finalized
   * state and is ready for output.
   * 
   * @throws Exception
   */
  void finalize() throws Exception;

  /**
   * Reset the graph to its initial empty state.
   */
  void clear();
}
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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Represents a distributed vertex. A vertex in a distributed graph partitioned
 * by vertex separators may belong to multiple partitions. The vertex separators
 * are set of vertices whose removal leads to disjoint partitions of the
 * original graph. Therefore, a vertex record maintains the global information
 * about a vertex in the distributed setting. Among the partitions which
 * contains a certain vertex, one of the partition will be the owner of that
 * vertex, and the vertex copy on other partitions are called mirrors.
 * 
 * @param <VidType>
 *          the type of vertex id.
 * @param <VertexData>
 *          the type of vertex data.
 */
public class VertexRecord<VidType, VertexData> {
  /** Default constructor of an empty vertex record. */
  public VertexRecord() {
  }

  /**
   * Creates a vertex record of a given vertex id.
   * 
   * @param gvid
   *          the global id of the vertex.
   */
  public VertexRecord(VidType gvid) {
    owner = -1;
    this.gvid = gvid;
    inEdges = 0;
    outEdges = 0;
  }

  /**
   * @return the number of mirror vertices.
   */
  public int numMirrors() {
    return mirrors.cardinality();
  }

  /**
   * @return the vertex id.
   */
  public VidType vid() {
    return gvid;
  }

  /**
   * @param id
   *          the new vertex id.
   */
  public void setVid(VidType id) {
    this.gvid = id;
  }

  /**
   * @return the number of edges whose target vertex equals to this vertex in
   *         the entire graph.
   */
  public int inEdges() {
    return inEdges;
  }

  /**
   * @param inEdges
   *          the number of edges whose target vertex equals to this vertex in
   *          the entire graph.
   */
  public void setInEdges(int inEdges) {
    this.inEdges = inEdges;
  }

  /**
   * @return the number of edges whose source vertex equals to this vertex in
   *         the entire graph.
   */
  public int outEdges() {
    return outEdges;
  }

  /**
   * the number of edges whose source vertex equals to this vertex in the entire
   * graph.
   * 
   * @param outEdges
   */
  public void setOutEdges(int outEdges) {
    this.outEdges = outEdges;
  }

  /**
   * @return the vertex data.
   */
  public VertexData vdata() {
    return vdata;
  }

  /**
   * @param vdata
   *          the new vertex data.
   */
  public void setVdata(VertexData vdata) {
    this.vdata = vdata;
  }

  /**
   * @return the owner partition id of the vertex.
   */
  public short owner() {
    return owner;
  }

  /**
   * @param owner
   *          the owner partition id of the vertex.
   */
  public void setOwner(short owner) {
    this.owner = owner;
  }

  /**
   * @return a list of representation of the mirrors of this vertex.
   */
  public List<Short> mirrorList() {
    if (mirrors == null)
      return new ArrayList<Short>();
    ArrayList<Short> ret = new ArrayList<Short>(mirrors.cardinality());
    for (int j = mirrors.nextSetBit(0); j >= 0; j = mirrors.nextSetBit(j + 1)) {
      ret.add((short) j);
    }
    return ret;
  }

  /**
   * @param mirrors
   *          a list representation of the mirrors of this vertex.
   * @param numProcs
   *          total number of partitions.
   */
  public void setMirrorsFromList(List<Integer> mirrors, int numProcs) {
    this.mirrors = new BitSet(numProcs);
    for (int j = 0; j < mirrors.size(); j++) {
      this.mirrors.set(mirrors.get(j));
    }
  }

  /**
   * @param mirrors
   *          a bitset representation of the mirrors.
   */
  public void setMirrors(BitSet mirrors) {
    this.mirrors = mirrors;
  }

  /**
   * @param i
   *          a new partition id to be added to the mirror list.
   */
  public void addMirror(short i) {
    this.mirrors.set(i);
  }

  /**
   * @param i
   *          the partition id to be removed from the mirror list.
   */
  public void removeMirror(short i) {
    this.mirrors.clear(i);
  }

  @Override
  public final String toString() {
    return (new JsonVrecordFormatter()).vrecordWriter(this).toString();
  }

  /**
   * Number of incoming and outgoing edges with respect to the entire graph.
   */
  private int inEdges, outEdges;

  /**
   * A bitset representing the mirrors of this vertex. The length of the bitset
   * equals to the total number of partitions, and the ith is set if and only if
   * partition i contains this vertex and partition i is not the owner of this
   * vertex.
   */
  private BitSet mirrors;

  /**
   * The id of the partition which owns this vertex.
   */
  private short owner;

  /**
   * The global id of this vertex.
   */
  private VidType gvid;

  /**
   * The data of this vertex.
   */
  private VertexData vdata;
}

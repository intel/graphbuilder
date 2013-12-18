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
package com.intel.hadoop.graphbuilder.graph.simplegraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;

import org.apache.log4j.Logger;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import com.intel.hadoop.graphbuilder.graph.Graph;
import com.intel.hadoop.graphbuilder.graph.VertexRecord;
import com.intel.hadoop.graphbuilder.graph.glgraph.GLGraph;

/**
 * This is a pre-finalized but post-partitioned adjacency format for GraphLab2.
 * The local edges are stored in an adjacency list. The advantage of this
 * representation lies in its simplicity, therefore easier to parallelize than
 * {@code GLGraph}. Also, it leaves the heavy duty of {@link GLGraph#finalize()}
 * to much more efficient C++ code on the GraphLab2 side.
 * 
 * @param <VidType>
 * @param <VertexData>
 * @param <EdgeData>
 */
public class SimpleGraphWithFastUtil<VidType, VertexData, EdgeData> implements
    Graph<VidType, VertexData, EdgeData> {

  private static final Logger LOG = Logger.getLogger(SimpleGraph.class);

  /** Creates an empty graph. */
  public SimpleGraphWithFastUtil() {
    numVertices = 0;
    numEdges = 0;

    adjList = new Int2ObjectOpenHashMap();
    edataList = new Int2ObjectOpenHashMap();
    vrecordList = new ArrayList<VertexRecord<VidType, VertexData>>();
  }

  /**
   * @return a list of ids of the vertices in the graph partition.
   */
  public IntArrayList vertices() {
    return new IntArrayList(adjList.keySet());
  }

  /**
   * @param v
   *          the source vertex of the edge list.
   * @return an ordered list of vertices u where there is an edge from v -> u.
   */
  public IntArrayList outEdgeTargetIds(int v) {
    return (IntArrayList)adjList.get(v);
  }

  /**
   * @param v
   *          the source vertex of the edge list.
   * @return an ordered list of edge data of the outgoing edges of v.
   */
  public FloatArrayList outEdgeData(int v) {
    return (FloatArrayList)edataList.get(v);
  }

  /**
   * Clear the edge data list.
   */
  public void clearEdataList() {
    Int2ObjectMap.FastEntrySet set = edataList.int2ObjectEntrySet();
    Iterator i = set.iterator();
    while (i.hasNext()) {
      Map.Entry me = (Map.Entry)i.next();
      FloatArrayList edata = (FloatArrayList)me.getValue();
      edata.clear();
    } 
    edataList.clear();
  }

  /**
   * Clear the adjacency list.
   */
  public void clearAdjList() {

    Int2ObjectMap.FastEntrySet set = adjList.int2ObjectEntrySet();
    Iterator i = set.iterator();
    while(i.hasNext()) {
        Map.Entry me = (Map.Entry)i.next();
        IntArrayList vids = (IntArrayList)me.getValue();
        vids.clear();
    }
    adjList.clear();
  }

  @Override
  public void reserveEdgeSpace(int numEdges) {
  }

  @Override
  public void reserveVertexSpace(int numVertices) {
    this.vrecordList.ensureCapacity(numVertices);
  }

  @Override
  public int pid() {
    return pid;
  }

  @Override
  public void setPid(int pid) {
    this.pid = pid;
  }

  @Override
  public int numVertices() {
    return numVertices;
  }

  @Override
  public int numEdges() {
    return numEdges;
  }


  @Override
  public void addEdges(List<VidType> sources, List<VidType> targets,
       List<EdgeData> edata) {

    for (int i = 0; i < sources.size(); i++)
      addEdge(sources.get(i), targets.get(i), edata.get(i));
  }

  @Override
  public void addEdge(VidType s, VidType t, EdgeData d)
  { 
    int source = Integer.parseInt(s.toString());
    int target = Integer.parseInt(t.toString());
    float data = Float.parseFloat(d.toString());

    if (adjList.containsKey(source)) {
      IntArrayList vids = (IntArrayList)adjList.get(source);
      vids.add(target);
      FloatArrayList edata = (FloatArrayList)edataList.get(source);
      edata.add(data);
    } else {
      IntArrayList vidlist = new IntArrayList();
      vidlist.add(target);
      FloatArrayList elist = new FloatArrayList();
      elist.add(data);
      adjList.put(source, vidlist);
      edataList.put(source, elist);
    }
    ++numEdges;
  }

  @Override
  public void addVertexRecord(VertexRecord<VidType, VertexData> vrecord) {
    vrecordList.add(vrecord);
  }

  @Override
  public void finalize() {
  }

  @Override
  public void clear() {
    clearEdataList();
    clearAdjList();
    vrecordList.clear();
    pid = 0;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("-------------Graph Stats--------------\n");
    builder.append("pid: " + pid + "\n");
    builder.append("numVertices: " + numVertices + "\n");
    builder.append("numEdges: " + numEdges + "\n");

    builder.append("-------------Edges---------------\n");

    builder.append("-------------Vertices---------------\n");
    for (int i = 0; i < vrecordList.size(); ++i) {
      builder.append(vrecordList.get(i).toString() + "\n");
    }
    return builder.toString();
  }

  private int pid;
  private int numVertices;
  private int numEdges;

  //vertex data does not consume large memory, so keep it..
  private ArrayList<VertexRecord<VidType, VertexData>> vrecordList;
  private Int2ObjectOpenHashMap adjList;
  private Int2ObjectOpenHashMap edataList;
  
}

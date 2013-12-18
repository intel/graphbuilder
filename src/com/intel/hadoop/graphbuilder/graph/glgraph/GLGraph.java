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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.graph.Graph;
import com.intel.hadoop.graphbuilder.graph.VertexRecord;
import com.intel.hadoop.graphbuilder.util.Parallel;
import com.intel.hadoop.graphbuilder.util.Parallel.Operation;

/**
 * This is equivalent to GraphLab2 distributed graph {@link http
 * ://code.google.com
 * /p/graphlabapi/source/browse/src/graphlab/graph/distributed_graph.hpp}. Most
 * of the implementation is identical to the C++ version in GraphLab2. Besides
 * the "set" methods defined in the {@code Graph} interface, this class also
 * provides "get" methods iterate over the incoming/outgoing edge list of a
 * given vertex.
 * 
 * @param <VidType>
 * @param <VertexData>
 * @param <EdgeData>
 */
public class GLGraph<VidType, VertexData, EdgeData> implements
    Graph<VidType, VertexData, EdgeData> {

  private static final Logger LOG = Logger.getLogger(GLGraph.class);

  public GLGraph() {
    numVertices = numEdges = 0;
    vid2lvid = new HashMap<VidType, Integer>();
    edgeInfo = new EdgeListStorage<EdgeData>();
    vrecordList = new ArrayList<VertexRecord>();
  }

  public int numEdges() {
    return numEdges;
  }

  /**
   * @param e
   *          A lazy edge object.
   * @return the edge data of EdgeType e.
   */
  public EdgeData edgeData(EdgeType e) {
    if (e.direction == EdgeType.DIR.EMPTY) {
      return null;
    } else if (e.direction == EdgeType.DIR.IN) {
      return edatalist.get(c2rMap.get(e.edgeid()));
    } else {
      return edatalist.get(e.edgeid());
    }
  }

  /**
   * @param e
   *          The lazy edge object.
   * @param value
   *          the new edge data.
   * @return the new edge data of EdgeType e.
   */
  public EdgeData setEdgeData(EdgeType e, EdgeData value) {
    if (e.direction == EdgeType.DIR.EMPTY) {
      return null;
    } else if (e.direction == EdgeType.DIR.IN) {
      return edatalist.set(c2rMap.get(e.edgeid()), value);
    } else {
      return edatalist.set(e.edgeid(), value);
    }
  }

  /**
   * @param gvid
   *          the global id of the vertex
   * @return The local vertex id of a vertex in this graph partition.
   */
  public int lvid(VidType gvid) {
    if (vid2lvid.containsKey(gvid))
      return vid2lvid.get(gvid);
    else
      return -1;
  }

  /**
   * @param vid
   *          the local vertex id.
   * @return Number of incoming edges of the vertex in this graph partition, NOT
   *         in the global graph.
   */
  public int numInEdges(int vid) {
    return csc.end(vid) - csc.begin(vid);
  }

  /**
   * @param vid
   *          the local vertex id.
   * @return Number of outgoing edges of the vertex in this graph partition, NOT
   *         in the global graph.
   */
  public int numOutEdges(int vid) {
    return csr.end(vid) - csr.begin(vid);
  }

  @Override
  public void reserveEdgeSpace(int numEdges) {
    edgeInfo.reserve(numEdges);
  }

  @Override
  public void reserveVertexSpace(int numVertices) {
    vid2lvid = new HashMap<VidType, Integer>(numVertices);
    vrecordList.ensureCapacity(numVertices);
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
  public void addEdges(List<VidType> sources, List<VidType> targets,
      List<EdgeData> edata) {
    for (int i = 0; i < sources.size(); ++i)
      addEdge(sources.get(i), targets.get(i), edata.get(i));
  }

  @Override
  public void addEdge(VidType source, VidType target, EdgeData edata) {
    int lsource, ltarget;
    if (vid2lvid.containsKey(source)) {
      lsource = vid2lvid.get(source);
    } else {
      lsource = vid2lvid.size();
      vid2lvid.put(source, lsource);
    }
    if (vid2lvid.containsKey(target)) {
      ltarget = vid2lvid.get(target);
    } else {
      ltarget = vid2lvid.size();
      vid2lvid.put(target, ltarget);
    }
    edgeInfo.addEdge(lsource, ltarget, edata);
    numVertices = vid2lvid.size();
    numEdges++;
    if (numEdges % 1000000 == 0) {
      LOG.debug("Graph added " + numEdges + " edges.");
    }
  }

  @Override
  public void addVertexRecord(VertexRecord<VidType, VertexData> vrecord) {
    vrecordList.add(vrecord);
  }

  public EdgeList inEdges(int vid) {
    return new EdgeList(vid, csc, EdgeType.DIR.IN);
  }

  public EdgeList outEdges(int vid) {
    return new EdgeList(vid, csr, EdgeType.DIR.OUT);
  }

  @Override
  public void finalize() throws Exception {
    if (finalized)
      return;

    boolean parallel = true;
    Parallel parfor = new Parallel();

    LOG.debug("Finalize graph of size: " + edgeInfo.size());
    ArrayList<Integer> permute = new ArrayList<Integer>(Collections.nCopies(
        numEdges, 0));
    ArrayList<AtomicInteger> counterArray = new ArrayList<AtomicInteger>(
        numVertices + 1);
    for (int j = 0; j < numVertices + 1; ++j)
      counterArray.add(new AtomicInteger(0));

    /* Construct the CSR */
    /* Sort and divide edges by source id using counting sort. */
    LOG.debug("Coutning sort source...");
    counting_sort(edgeInfo.sources, counterArray, permute);
    final CompareByVid cmpByTarget = new CompareByVid(edgeInfo.targets);

    final ArrayList<List<Integer>> dummyList = new ArrayList<List<Integer>>(
        numVertices);
    /* Sort each part (of the same source) by its target. */
    if (parallel) {
      LOG.debug("Parallel sort target within source...");
      for (int j = 0; j < numVertices; ++j)
        dummyList.add(permute.subList(counterArray.get(j).get(), counterArray
            .get(j + 1).get()));
      parfor.For(dummyList, new Operation<List<Integer>>() {
        @Override
        public void perform(List<Integer> pParameter, int idx) {
          // TODO Auto-generated method stub
          java.util.Collections.sort(pParameter, cmpByTarget);
        }
      });
    } else {
      for (int j = 0; j < numVertices; ++j) {
        java.util.Collections.sort(permute.subList(counterArray.get(j).get(),
            counterArray.get(j + 1).get()), cmpByTarget);
      }
    }

    /*
     * Shuffle in place the sources, targets, and edatalist using the permute
     * index.
     */
    edgeInfo.inplace_shuffle(permute);

    /* Fill in the CSR data structure. */
    csr = new SparseGraphStruct(numVertices, edgeInfo.sources, edgeInfo.targets);

    /* Construct the CSC */
    /* Sort and divide edges by source id using counting sort. */
    LOG.debug("Coutning sort target...");
    counting_sort(edgeInfo.targets, counterArray, permute);
    final CompareByVid cmpBySource = new CompareByVid(edgeInfo.sources);
    if (parallel) {
      LOG.debug("Parallel sort source within target...");
      dummyList.clear();
      for (int j = 0; j < numVertices; ++j)
        dummyList.add(permute.subList(counterArray.get(j).get(), counterArray
            .get(j + 1).get()));
      parfor.For(dummyList, new Operation<List<Integer>>() {
        @Override
        public void perform(List<Integer> pParameter, int idx) {
          java.util.Collections.sort(pParameter, cmpBySource);
        }
      });
    } else {
      for (int j = 0; j < numVertices; ++j) {
        java.util.Collections.sort(permute.subList(counterArray.get(j).get(),
            counterArray.get(j + 1).get()), cmpBySource);
      }
    }

    /* Shuffle out of place the sources. */
    final ArrayList<Integer> shuffledSource = new ArrayList<Integer>(
        Collections.nCopies(edgeInfo.sources.size(), 0));

    parfor.For(permute, new Operation<Integer>() {
      @Override
      public void perform(Integer val, int idx) {
        shuffledSource.set(idx, edgeInfo.sources.get(val));
      }
    });

    edgeInfo.sources = shuffledSource;

    // Fill in the CSC data structure
    List<Integer> transformedTargets = new ArrayList<Integer>(
        edgeInfo.targets.size());
    for (int j = 0; j < edgeInfo.targets.size(); ++j) {
      transformedTargets.add(edgeInfo.targets.get(permute.get(j)));
    }
    csc = new SparseGraphStruct(numVertices, transformedTargets,
        edgeInfo.sources);

    c2rMap = permute;
    edatalist = edgeInfo.edata;
    finalized = true;
    parfor.close();
  }

  /**
   * @return the map from global vid to local vid in this graph partition.
   */
  public Map<VidType, Integer> vid2lvid() {
    return vid2lvid;
  }

  /**
   * @return a list of edgedata in this graph partition.
   */
  public List<EdgeData> edatalist() {
    return edatalist;
  }

  @Override
  public void clear() {
    csr.clear();
    csc.clear();
    c2rMap.clear();
    edatalist.clear();
    vrecordList.clear();
    vid2lvid.clear();
    edgeInfo.clear();
    numVertices = 0;
    numEdges = 0;
    finalized = false;
    pid = 0;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("-------------Graph Stats--------------\n");
    builder.append("pid: " + pid + "\n");
    builder.append("numVertices: " + numVertices + "\n");
    builder.append("numEdges: " + numEdges + "\n");
    builder.append("-------------Vid2lvid---------------\n");
    Iterator<Entry<VidType, Integer>> iter = vid2lvid.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<VidType, Integer> e = iter.next();
      builder.append(e.getKey().toString() + ":\t" + e.getValue() + "\n");
    }
    builder.append("-------------Edges---------------\n");
    for (int i = 0; i < numEdges; ++i) {
      EdgeList l = outEdges(i);
      Iterator<EdgeType> edgeiter = l.iterator();
      while (edgeiter.hasNext()) {
        EdgeType e = edgeiter.next();
        builder.append("(" + e.source() + ", " + e.target() + ", "
            + edgeData(e).toString() + ")\n");
      }
    }
    builder.append("-------------Vertices---------------\n");
    for (int i = 0; i < vrecordList.size(); ++i) {
      builder.append(vrecordList.get(i).toString() + "\n");
    }
    return builder.toString();
  }

  /**
   * @return the CSR representation of the graph.
   */
  public SparseGraphStruct csr() {
    return csr;
  }

  /**
   * @return the CSC representation of the graph.
   */
  public SparseGraphStruct csc() {
    return csc;
  }

  /**
   * @return the edge index mapping from the CSC to CSR.
   */
  public List<Integer> c2rMap() {
    return c2rMap;
  }

  /*
   * =================Private helper functions========================
   */
  /**
   * Given a reference ArrayList<Integer> valueList, compare two integers i,j by
   * valueList[i], and valueList[j].
   * 
   */
  private class CompareByVid implements Comparator<Integer> {
    /**
     * Constructs a comparator with a given reference list.
     * 
     * @param val
     */
    public CompareByVid(ArrayList<Integer> val) {
      valueList = val;
    }

    @Override
    public int compare(Integer arg0, Integer arg1) {
      Integer v0 = valueList.get((arg0));
      Integer v1 = valueList.get((arg1));
      return v0.compareTo(v1);
    }

    /** The reference list for comparison. */
    private ArrayList<Integer> valueList;
  }

  /**
   * Performs a counting sort on valueArray and stores the result in
   * counterArray and permute.
   * 
   * @param valueArray
   *          the array of elements to be sorted.
   * @param counterArray
   *          the array counting the occurence of each element.
   * @param permute
   *          the permutation array corresponds to the sorting operation.
   * @param <valueType>
   *          the type of the element to be sorted.
   */
  private <valueType> void counting_sort(ArrayList<valueType> valueArray,
      final ArrayList<AtomicInteger> counterArray,
      final ArrayList<Integer> permute) {

    assert permute.size() == valueArray.size();
    Parallel parfor = new Parallel();
    for (int j = 0; j < counterArray.size(); j++)
      counterArray.get(j).set(0);

    for (int j = 0; j < permute.size(); j++)
      permute.set(j, 0);

    parfor.For(valueArray, new Operation<valueType>() {
      @Override
      public void perform(valueType val, int idx) {
        counterArray.get((Integer) val).incrementAndGet();
      }

    });

    for (int j = 1; j < counterArray.size(); j++)
      counterArray.get(j).addAndGet(counterArray.get(j - 1).get());

    parfor.For(valueArray, new Operation<valueType>() {
      @Override
      public void perform(valueType val, int idx) {
        permute.set(counterArray.get((Integer) val).decrementAndGet(), idx);
      }

    });

    parfor.close();
  }

  /** Partition id of this graph. */
  private int pid;
  /** Number of local vertices. */
  private int numVertices;
  /** Number of local edges. */
  private int numEdges;
  /** Flag of finalized state. */
  private boolean finalized;

  /** Internal storage of the edge data. */
  private ArrayList<EdgeData> edatalist;
  /** Internal storage of the vertex records, with length = #vertices. */
  private ArrayList<VertexRecord> vrecordList;
  /** Temporary storage of edges. */
  private EdgeListStorage<EdgeData> edgeInfo;
  /** Map from global vid to local vid. */
  private HashMap<VidType, Integer> vid2lvid;
  /** CSR representation of the adjacency structure. */
  private SparseGraphStruct csr;
  /** CSC representation of the adjacency structure. */
  private SparseGraphStruct csc;
  /** EdgeData index mapping from CSC to CSR, with length = #edges. */
  private ArrayList<Integer> c2rMap;

}

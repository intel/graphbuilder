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
package com.intel.hadoop.graphbuilder.test.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.junit.Test;

import com.intel.hadoop.graphbuilder.graph.Edge;
import com.intel.hadoop.graphbuilder.graph.Graph;
import com.intel.hadoop.graphbuilder.graph.VertexRecord;
import com.intel.hadoop.graphbuilder.graph.glgraph.EdgeList;
import com.intel.hadoop.graphbuilder.graph.glgraph.EdgeType;
import com.intel.hadoop.graphbuilder.graph.glgraph.GLGraph;
import com.intel.hadoop.graphbuilder.parser.BasicGraphParser;
import com.intel.hadoop.graphbuilder.parser.EmptyParser;
import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;
import com.intel.hadoop.graphbuilder.parser.IntParser;
import com.intel.hadoop.graphbuilder.types.EmptyType;
import com.intel.hadoop.graphbuilder.types.IntType;

/**
 * Unit test for GLGraph.
 *
 *
 */
public class GLGraphTest {

  @Test
  public void testEmptyGraph() throws Exception {
    Graph<Integer, Integer, Integer> myGraph = new GLGraph<Integer, Integer, Integer>();
    assertEquals("Number of edges", 0, myGraph.numEdges());
    assertEquals("Number of vertices", 0, myGraph.numVertices());
    myGraph.finalize();
  }

  @Test
  public void testSmallGraph() throws Exception {
    int numEdges = 5, numVertices = 6;
    ArrayList<Integer> sourceInput = new ArrayList<Integer>(Arrays.asList(0, 0,
        2, 3, 0));
    ArrayList<Integer> targetInput = new ArrayList<Integer>(Arrays.asList(3, 6,
        3, 4, 1));
    ArrayList<Integer> valueInput = new ArrayList<Integer>(Arrays.asList(1, 2,
        3, 4, 5));

    GLGraph<Integer, Integer, Integer> myGraph = new GLGraph<Integer, Integer, Integer>();
    for (int i = 0; i < numEdges; i++)
      myGraph
          .addEdge(sourceInput.get(i), targetInput.get(i), valueInput.get(i));

    assertEquals("Num edges: ", numEdges, myGraph.numEdges());
    assertEquals("Num vertices: ", numVertices, myGraph.numVertices());

    myGraph.finalize();

    ArrayList<Integer> rowIndexExpect = new ArrayList<Integer>(Arrays.asList(0,
        3, -1, 4, -1, -1));
    ArrayList<Integer> colIndexExpect = new ArrayList<Integer>(Arrays.asList(1,
        2, 5, 4, 1));
    assertEquals("CSR row", rowIndexExpect, myGraph.csr().getRowIndex());
    assertEquals("CSR col", colIndexExpect, myGraph.csr().getColIndex());

    rowIndexExpect = new ArrayList<Integer>(Arrays.asList(-1, 0, 2, -1, 3, 4));
    colIndexExpect = new ArrayList<Integer>(Arrays.asList(0, 3, 0, 1, 0));
    assertEquals("CSC row", rowIndexExpect, myGraph.csc().getRowIndex());
    assertEquals("CSC col", colIndexExpect, myGraph.csc().getColIndex());
    // System.out.println(myGraph.toJSONObj().toJSONString());

    // check #inedges(vid), #outedges(vid)
    ArrayList<Integer> numInEdgesExpect = new ArrayList<Integer>(Arrays.asList(
        0, 1, 0, 2, 1, 0, 1));
    ArrayList<Integer> numOutEdgesExpect = new ArrayList<Integer>(
        Arrays.asList(3, 0, 1, 1, 0, 0, 0));
    ArrayList<Integer> testNumEdges = new ArrayList<Integer>();

    int maxid = Math.max(Collections.max(sourceInput),
        Collections.max(targetInput));
    for (int i = 0; i <= maxid; ++i) {
      int lvid = myGraph.lvid(i);
      if (lvid >= 0)
        testNumEdges.add(myGraph.numInEdges(myGraph.lvid(i)));
      else
        testNumEdges.add(myGraph.numInEdges(0));
    }
    assertEquals("Num in edges", testNumEdges, numInEdgesExpect);
    testNumEdges.clear();
    for (int i = 0; i <= maxid; ++i) {
      int lvid = myGraph.lvid(i);
      if (lvid >= 0)
        testNumEdges.add(myGraph.numOutEdges(lvid));
      else
        testNumEdges.add(0);
    }
    // assertEquals("Num out edges", testNumEdges, numOutEdgesExpect);

    // check inedges(vid), outedges(vid)
    // iterate out edges of 0: 3, 6, 1
    HashSet<Integer> testEdges = new HashSet<Integer>();
    EdgeList list = myGraph.outEdges(myGraph.lvid(0));
    Iterator<EdgeType> iter = list.iterator();
    testEdges.add(myGraph.lvid(3));
    testEdges.add(myGraph.lvid(6));
    testEdges.add(myGraph.lvid(1));
    while (iter.hasNext()) {
      EdgeType e = iter.next();
      assertEquals(e.source(), myGraph.lvid(0));
      assertTrue(testEdges.contains(e.target()));
    }
    // iterate out edges of 1: null
    list = myGraph.outEdges(myGraph.lvid(1));
    assertTrue(list.isEmpty());

    // iterate in edges of 3: 0, 2
    list = myGraph.inEdges(myGraph.lvid(3));
    testEdges.clear();
    testEdges.add(myGraph.lvid(0));
    testEdges.add(myGraph.lvid(2));
    iter = list.iterator();
    while (iter.hasNext()) {
      EdgeType e = iter.next();
      assertEquals(e.target(), myGraph.lvid(3));
      assertTrue(testEdges.contains(e.source()));
    }
    // iterate in edges of 2: null
    list = myGraph.inEdges(myGraph.lvid(2));
    assertTrue(list.isEmpty());
  }

  @Test
  public void buildGraphExample() throws Exception {
    int numEdges = 5, numVertices = 6;
    ArrayList<Integer> sourceInput = new ArrayList<Integer>(Arrays.asList(0, 0,
        2, 3, 0));
    ArrayList<Integer> targetInput = new ArrayList<Integer>(Arrays.asList(3, 5,
        3, 4, 1));
    ArrayList<Integer> valueInput = new ArrayList<Integer>(Arrays.asList(1, 2,
        3, 4, 5));

    ArrayList<Integer> vertexInput = new ArrayList<Integer>(Arrays.asList(0, 1,
        2, 3, 4, 5));
    HashMap<Integer, Integer> inEdgesTable = new HashMap<Integer, Integer>();
    HashMap<Integer, Integer> outEdgesTable = new HashMap<Integer, Integer>();
    for (int i = 0; i < numVertices; ++i) {
      inEdgesTable.put(i, 0);
      outEdgesTable.put(i, 0);
    }

    Graph<Integer, Integer, Integer> myGraph = new GLGraph<Integer, Integer, Integer>();
    for (int i = 0; i < numEdges; i++) {
      int source = sourceInput.get(i);
      int target = targetInput.get(i);
      int val = valueInput.get(i);
      inEdgesTable.put(target, inEdgesTable.get(target) + 1);
      outEdgesTable.put(source, outEdgesTable.get(source) + 1);
      myGraph.addEdge(source, target, val);
    }
    for (int i = 0; i < numVertices; i++) {
      VertexRecord<Integer, Integer> vrec = new VertexRecord<Integer, Integer>(
          i);
      vrec.setOwner((short) 0);
      vrec.setVdata(vertexInput.get(i));
      vrec.setInEdges(inEdgesTable.get(i));
      vrec.setOutEdges(outEdgesTable.get(i));
      vrec.setMirrors(new BitSet());
      myGraph.addVertexRecord(vrec);
    }

    myGraph.finalize();

    // System.out.println(myGraph.toString());
  }

  public void testGoogleGraph() throws Exception {
    String path = "testgraphs/google/web-Google.txt";
    Graph<IntType, EmptyType, EmptyType> myGraph = new GLGraph<IntType, EmptyType, EmptyType>();
    GraphParser graphparser = new BasicGraphParser();
    FieldParser vidparser = new IntParser();
    FieldParser dataparser = new EmptyParser();

    try {
      BufferedReader br = new BufferedReader(new FileReader(path));
      int numEdges = 0;

      String line = br.readLine();
      while (line != null) {
        if (graphparser.isEdgeData(line)) {
          Edge<IntType, EmptyType> e = graphparser.parseEdge(line, vidparser,
              dataparser);
          myGraph.addEdge(e.source(), e.target(), e.EdgeData());
          numEdges++;
        }
        line = br.readLine();
      }
      System.out.println("Graph finalize...");
      myGraph.finalize();
      System.out.println("Finish finalize...");
      assertEquals("NumAddedEdges:", numEdges, myGraph.numEdges());
      assertEquals("NumEdges:", 5105039, myGraph.numEdges());
      assertEquals("NumVertices:", 875713, myGraph.numVertices());
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /*
   * public void testCountingSort() { Graph<Integer, Integer, Integer> myGraph =
   * new Graph<Integer, Integer, Integer>();
   * 
   * ArrayList<Integer> values = new ArrayList<Integer>(
   * Arrays.asList(4,2,1,5,2,3,4,1,6,8)); AtomicIntegerArray counterArray = new
   * AtomicIntegerArray(11); ArrayList<Integer> permute = new
   * ArrayList<Integer>(Collections.nCopies(10, 0));
   * myGraph.counting_sort(values, counterArray, permute);
   * 
   * 
   * ArrayList<Integer> expectPermute = new ArrayList<Integer>(Arrays.asList(7,
   * 2, 4, 1, 5, 6, 0, 3, 8, 9)); assertEquals("Permute array", expectPermute,
   * permute); }
   */

}

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

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

import com.intel.hadoop.graphbuilder.graph.glgraph.SparseGraphStruct;

/**
 * Unit test for SparseGraphStructs.
 *
 */
public class SparseGraphStructTest {

  @Test
  public void test() {
    testEmptyGraph(0);
    testEmptyGraph(10);
    testEmptyGraph(100);
    testSmallGraph();
  }

  public void testEmptyGraph(int numVertices) {
    SparseGraphStruct tester = new SparseGraphStruct(numVertices);
    assertEquals("Num edges", 0, tester.numEdges());
    assertEquals("Num vertices", numVertices, tester.numVertices());
  }

  public void testSmallGraph() {
    ArrayList<Integer> source = new ArrayList<Integer>(Arrays.asList(0, 0, 0,
        2, 3));
    ArrayList<Integer> target = new ArrayList<Integer>(Arrays.asList(1, 3, 6,
        3, 4));
    SparseGraphStruct myCSR = new SparseGraphStruct(7, source, target);
    ArrayList<Integer> rowIndexExpect = new ArrayList<Integer>(Arrays.asList(0,
        -1, 3, 4, -1, -1, -1));
    ArrayList<Integer> colIndexExpect = new ArrayList<Integer>(Arrays.asList(1,
        3, 6, 3, 4));
    assertEquals("CSR row", rowIndexExpect, myCSR.getRowIndex());
    assertEquals("CSR col", colIndexExpect, myCSR.getColIndex());

    source = new ArrayList<Integer>(Arrays.asList(0, 0, 2, 3, 0));
    target = new ArrayList<Integer>(Arrays.asList(1, 3, 3, 4, 6));
    SparseGraphStruct myCSC = new SparseGraphStruct(7, target, source);
    rowIndexExpect = new ArrayList<Integer>(Arrays.asList(-1, 0, -1, 1, 3, -1,
        4));
    colIndexExpect = new ArrayList<Integer>(Arrays.asList(0, 0, 2, 3, 0));
    assertEquals("CSC row", rowIndexExpect, myCSC.getRowIndex());
    assertEquals("CSC col", colIndexExpect, myCSC.getColIndex());
  }

}

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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.intel.hadoop.graphbuilder.graph.simplegraph.SimpleGraph;
import com.intel.hadoop.graphbuilder.graph.simplegraph.SimpleJsonFormatter;

/**
 * Unit test for SimpleGraph.
 * 
 */
public class SimpleGraphTest {

  @Test
  public void testEmptyGraph() {
    SimpleGraph mygraph = new SimpleGraph();
    assertEquals(mygraph.numEdges(), 0);
  }

  @Test
  public void testSmallGraph() {
    SimpleGraph mygraph = new SimpleGraph();
    List<Integer> sources = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> targets = Arrays.asList(5, 4, 3, 2, 1);
    List<Character> edata = Arrays.asList('a', 'b', 'c', 'd', 'e');

    assertEquals(mygraph.numEdges(), 0);
    for (int i = 0; i < sources.size(); i++) {
      mygraph.addEdge(sources.get(i), targets.get(i), edata.get(i));
    }
    assertEquals(mygraph.numEdges(), sources.size());
  }

  /*public void testGraphFormatter() {
    SimpleGraph mygraph = new SimpleGraph();
    List<Integer> sources = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> targets = Arrays.asList(5, 4, 3, 2, 1);
    List<String> edata = Arrays.asList("a", "b", "c", "d", "e");

    assertEquals(mygraph.numEdges(), 0);
    mygraph.addEdges(sources, targets, edata);
    assertEquals(mygraph.numEdges(), sources.size());

    SimpleJsonFormatter formatter = new SimpleJsonFormatter();
    String s = formatter.structWriter(mygraph).toString();
    String expected = "{\"source\":1,\"targets\":[5]}\n"
        + "{\"source\":2,\"targets\":[4]}\n"
        + "{\"source\":3,\"targets\":[3]}\n"
        + "{\"source\":4,\"targets\":[2]}\n"
        + "{\"source\":5,\"targets\":[1]}\n";
    assertEquals(s, expected);

    s = formatter.edataWriter(mygraph).toString();
    expected = "[\"a\",\"b\",\"c\",\"d\",\"e\"]";
    assertEquals(expected, s);
  }*/

}

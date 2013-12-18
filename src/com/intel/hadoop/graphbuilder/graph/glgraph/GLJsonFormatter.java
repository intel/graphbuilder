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

import java.io.IOException;
import java.io.StringWriter;

import net.minidev.json.JSONObject;

import com.intel.hadoop.graphbuilder.graph.Graph;
import com.intel.hadoop.graphbuilder.graph.EdgeFormatter;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * A JSON encoding of GLGraph.
 * 
 */
public class GLJsonFormatter implements EdgeFormatter {


  public void set(String name, OutputCollector out) 
  {

    //to be implemented
  }

  /**
   * @param g
   * @return A JSON string of the vid2lvid map of a GLGraph.
   */
  public StringWriter vid2lvidWriter(GLGraph g) {
    JSONObject obj = new JSONObject();
    obj.put("vid2lvid", g.vid2lvid());
    StringWriter out = new StringWriter();
    try {
      obj.writeJSONString(out);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return out;
  }

  @Override
  public StringWriter edataWriter(Graph g) {
    JSONObject obj = new JSONObject();
    obj.put("edataList", ((GLGraph) g).edatalist());
    StringWriter out = new StringWriter();
    try {
      obj.writeJSONString(out);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return out;
  }

  @Override
  public StringWriter structWriter(Graph graph) {
    JSONObject obj = new JSONObject();
    GLGraph g = (GLGraph) graph;
    obj.put("numVertices", g.numVertices());
    obj.put("numEdges", g.numEdges());
    obj.put("csr", g.csr().toJSONObj());
    obj.put("csc", g.csc().toJSONObj());
    obj.put("c2rMap", g.c2rMap());
    StringWriter out = new StringWriter();
    try {
      obj.writeJSONString(out);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return out;
  }
}

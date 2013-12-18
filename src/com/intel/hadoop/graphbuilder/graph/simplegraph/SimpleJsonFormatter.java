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

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import com.intel.hadoop.graphbuilder.graph.Graph;
import com.intel.hadoop.graphbuilder.graph.EdgeFormatter;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * A JSON encoding of {@code SimpleGraph} or {@code SimpleSubGraph}.
 * 
 */
public class SimpleJsonFormatter implements EdgeFormatter {

 
  @Override
  public void set(String name, OutputCollector out) 
  {
    this.filename = new Text(name);
    this.output = out;
  }

  @Override
  public StringWriter structWriter(Graph g) {
    JSONObject obj = new JSONObject();
    //dont need StringWriter with streamed JSON
    StringWriter out = new StringWriter();
    int count = 0;
    List sortedKey = ((SimpleGraph) g).vertices();
    Collections.sort(sortedKey);
    try {
      for (int i = 0; i < sortedKey.size(); i++) {
        obj.clear();
        obj.put("source", sortedKey.get(i));
        obj.put("targets", ((SimpleGraph) g).outEdgeTargetIds(sortedKey.get(i)));
        if (output!=null) {
          output.collect(filename, new Text(obj.toString()));
        } 
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return out;
  }

  public StringWriter edataWriter(Graph g) {
    //dont need StringWriter with streamed JSON
    StringWriter out = new StringWriter();
    int count = 0;
    List sortedKey = ((SimpleGraph) g).vertices();
    Collections.sort(sortedKey);
    List buffer = new ArrayList();
    try {
      for (int i = 0; i < sortedKey.size(); i++) {
        List data = (List) ((SimpleGraph) g).outEdgeData(sortedKey.get(i));
        buffer.addAll(data);
        if (buffer.size() > recordPerLine) {
          if (output!=null)
            output.collect(filename, new Text(buffer.toString()));
          buffer.clear();
        }
      }
      if(output!=null)
        output.collect(filename, new Text(buffer.toString()));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return out;
  }

  private static int recordPerLine = 1000;
  private Text filename;
  private OutputCollector output;
}

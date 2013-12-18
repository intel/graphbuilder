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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import com.intel.hadoop.graphbuilder.graph.Graph;
import com.intel.hadoop.graphbuilder.graph.EdgeFormatter;

/**
 * A JSON encoding of {@code SimpleGraphWithFastUtil} or {@code SimpleSubGraphWithFastUtil}.
 *
 */

public class SimpleJsonFormatterWithFastUtil implements EdgeFormatter {

  @Override
  public void set(String name, OutputCollector out) {
    this.output = out;
    this.filename = new Text(name);    
  }

  @Override
  public StringWriter structWriter(Graph g) {
    JSONObject obj = new JSONObject(); 
    // dont need StringWriter with streaming JSON
    StringWriter out = new StringWriter();
    int count = 0;

    if (sortedKey == null) {
        sortedKey = ((SimpleGraphWithFastUtil) g).vertices();
        Collections.sort(sortedKey);
    }
    try {
      for (int i = 0; i < sortedKey.size(); i++) {
        obj.clear();
        obj.put("source", sortedKey.get(i));
        obj.put("targets", ((SimpleGraphWithFastUtil) g).outEdgeTargetIds(sortedKey.get(i)));
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
    //dont need StringWriter with streaming JSON
    StringWriter out = new StringWriter(); 
    int count = 0;
    if (sortedKey==null) {
        sortedKey = ((SimpleGraphWithFastUtil) g).vertices();
        Collections.sort(sortedKey);
    } 
    FloatArrayList buffer = new FloatArrayList();
    try {
      for (int i = 0; i < sortedKey.size(); i++) {
        FloatArrayList data = (FloatArrayList) ((SimpleGraphWithFastUtil) g).outEdgeData(sortedKey.get(i));
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
  private IntArrayList sortedKey;
  private OutputCollector output;
  private Text filename;
}

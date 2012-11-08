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
package com.intel.hadoop.graphbuilder.preprocess.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.intel.hadoop.graphbuilder.preprocess.functional.Functional;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.VertexEdgeUnionType;
import com.intel.hadoop.graphbuilder.util.Pair;

/**
 * The Reducer class applies user defined {@code Functional}s to reduce
 * duplicate edges and vertices. If no such {@code Functional} is provide, it
 * outputs the first instance and discards the rest with the same identifier. It
 * also discards self edges: v - > v. An option for discard bidirectional edge
 * is provided by {@code cleanBidirectionalEdge(boolean)}.
 * <p>
 * Output directory structure:
 * <ul>
 * <li>$outputdir/edata contains edge data output</li>
 * <li>$outputdir/vdata contains vertex data output</li>
 * </ul>
 * </p>
 * 
 */
public class CreateGraphReducer extends MapReduceBase implements
    Reducer<IntWritable, VertexEdgeUnionType, Text, Text> {
  public static enum CREATE_GRAPH_COUNTER {
    NUM_VERTICES, NUM_EDGES
  };

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    this.valClass = job.getMapOutputValueClass();
    this.noBidir = job.getBoolean("noBidir", false);
    try {
      if (job.get("EdgeFunc") != null) {
        this.EdgeFunc = (Functional) Class.forName(job.get("EdgeFunc"))
            .newInstance();
        this.EdgeFunc.configure(job);
      }
      if (job.get("VertexFunc") != null) {
        this.VertexFunc = (Functional) Class.forName(job.get("VertexFunc"))
            .newInstance();
        this.VertexFunc.configure(job);
      }
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void reduce(IntWritable key, Iterator<VertexEdgeUnionType> iter,
      OutputCollector<Text, Text> out, Reporter reporter) throws IOException {

    VertexEdgeUnionType next;
    HashMap<Pair<Object, Object>, Writable> edgeset = new HashMap();
    HashMap<Object, Writable> vertexset = new HashMap();

    while (iter.hasNext()) {
      next = iter.next();
      // Apply reduce on vertex
      if (next.flag() == VertexEdgeUnionType.VERTEXVAL) {
        Object vid = next.vertex().vid();
        if (vertexset.containsKey(vid)) { // duplicate vertex
          if (VertexFunc != null)
            vertexset.put(vid,
                VertexFunc.reduce(next.vertex().vdata(), vertexset.get(vid)));
        } else {
          if (VertexFunc != null)
            vertexset.put(vid,
                VertexFunc.reduce(next.vertex().vdata(), VertexFunc.base()));
          else
            vertexset.put(vid, next.vertex().vdata());
        }
      } else {
        // Apply reduce on edges, remove self and (or merge) duplicate edges.
        // Optionally remove bidirectional edge.
        Pair p = new Pair(next.edge().source(), next.edge().target());

        // self edge
        if (p.getL().equals(p.getR()))
          continue;

        // duplicate edge
        if (edgeset.containsKey(p)) {
          if (EdgeFunc != null)
            edgeset.put(p,
                EdgeFunc.reduce(next.edge().EdgeData(), edgeset.get(p)));
        } else {
          if (EdgeFunc != null)
            edgeset.put(p,
                EdgeFunc.reduce(next.edge().EdgeData(), EdgeFunc.base()));
          else
            edgeset.put(p, next.edge().EdgeData());
        }
      }
    }

    int nverts = 0;
    int nedges = 0;

    // Output vertex records
    Iterator<Entry<Object, Writable>> vertexiter = vertexset.entrySet()
        .iterator();
    while (vertexiter.hasNext()) {
      Entry e = vertexiter.next();
      out.collect(new Text("vdata"), new Text(e.getKey().toString() + "\t"
          + e.getValue().toString()));
      nverts++;
    }
    reporter.incrCounter(CREATE_GRAPH_COUNTER.NUM_VERTICES, nverts);

    // Output edge records
    Iterator<Entry<Pair<Object, Object>, Writable>> edgeiter = edgeset
        .entrySet().iterator();
    while (edgeiter.hasNext()) {
      Entry<Pair<Object, Object>, Writable> e = edgeiter.next();
      if (noBidir && edgeset.containsKey(e.getKey().reverse())) {
        continue;
      } else {
        out.collect(new Text("edata"), new Text(e.getKey().getL() + "\t"
            + e.getKey().getR() + "\t" + e.getValue().toString()));
      }
      nedges++;
    }
    reporter.incrCounter(CREATE_GRAPH_COUNTER.NUM_EDGES, nedges);
  }

  protected boolean noBidir;
  protected Class keyClass;
  protected Class valClass;
  protected Functional EdgeFunc;
  protected Functional VertexFunc;
}

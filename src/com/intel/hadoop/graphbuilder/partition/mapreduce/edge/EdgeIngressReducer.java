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
package com.intel.hadoop.graphbuilder.partition.mapreduce.edge;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.graph.Graph;
import com.intel.hadoop.graphbuilder.graph.GraphOutput;
import com.intel.hadoop.graphbuilder.graph.JsonVrecordFormatter;
import com.intel.hadoop.graphbuilder.graph.VertexRecord;
import com.intel.hadoop.graphbuilder.graph.VrecordFormatter;
import com.intel.hadoop.graphbuilder.graph.simplegraph.SimpleGraph;
import com.intel.hadoop.graphbuilder.graph.simplegraph.SimpleGraphOutput;
import com.intel.hadoop.graphbuilder.graph.simplegraph.SimpleJsonFormatter;
import com.intel.hadoop.graphbuilder.graph.simplegraph.SimpleSubGraph;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.CombinedEdgeValueType;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.CombinedVrecordValueType;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.IngressKeyType;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.IngressValueType;
import com.intel.hadoop.graphbuilder.types.Mergable;

/**
 * This reduce task has 2 subroutines: edges are reduced into a {@code Graph}
 * and vertices are reduced into {@code VertexRecord}. The reducer executes one
 * of the 2 reduce methods based on the key type:
 * <ul>
 * <li>For EdgeType key, the reduce function has type: pid * List<EdgeTypeValue>
 * -> {@code Graph}.</li>
 * <li>For non-EdgeType key, the reduce function has type: vid *
 * List<VertexRecordValue> -> {@code Graph}</li>.
 * </ul>
 * <p>
 * Output format and directory structures are determined by the choice of
 * {@code Graph} and its corresponding {@code GraphFormat} and
 * {@code GraphOutput}.
 * </p>
 * The current reducer uses {@code SimpleSubGraph}, {@code SimpleJsonFormatter},
 * and {@code SimpleGraphOutput}.
 * 
 * @see SimpleSubGraph
 * @see SimpleGraph
 * @see SimpleJSONFormatter
 * @see SimpleGraphOutput
 * @param <VidType>
 * @param <VertexData>
 * @param <EdgeData>
 * @param <KeyType>
 * @param <ValueType>
 */
public class EdgeIngressReducer<VidType extends WritableComparable<VidType>, VertexData extends Writable, EdgeData extends Writable, KeyType extends IngressKeyType<VidType>, ValueType extends IngressValueType<VidType, VertexData, EdgeData>>
    extends MapReduceBase implements Reducer<KeyType, ValueType, Text, Text> {

  private static final Logger LOG = Logger.getLogger(EdgeIngressReducer.class);

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    this.numProcs = job.getInt("numProcs", 1);
    this.subpartPerPartition = job.getInt("subpartPerPartition", 1);
    // Switch to GLGraph by uncommenting the next line.
    // graphOutput = new GLGraphOutput(numProcs);
    graphOutput = new SimpleGraphOutput();
    graphOutput.configure(job);
  }

  @Override
  public void reduce(KeyType key, Iterator<ValueType> iter,
      OutputCollector<Text, Text> out, Reporter reporter) throws IOException {

    if (key.flag() == IngressKeyType.EDGEKEY) {
      try {
        reduceEdge(key.pid(), iter, out, reporter);
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else if (key.flag() == IngressKeyType.VERTEXKEY) {
      reduceVertex(key.vid(), iter, reporter);
      VrecordFormatter vformatter = new JsonVrecordFormatter();
      out.collect(new Text("vrecord"),
          new Text(vformatter.vrecordWriter(vrecord).toString()));
    } else {
      LOG.error("Unknown key type: " + key.flag());
    }
  }

  @Override
  public void close() throws IOException {
    graphOutput.close();
  }

  /**
   * Reduce a list of EdgeValues into a graph and output the graph.
   * 
   * @param pid
   * @param iter
   * @param out
   * @param reporter
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  protected void reduceEdge(int pid, Iterator<ValueType> iter,
      OutputCollector<Text, Text> out, Reporter reporter) throws Exception {

    // Switch to GLGraph by uncommenting the next line.
    // myGraph = new GLGraph<VidType, VertexData, EdgeData>();
    myGraph = new SimpleSubGraph<VidType, VertexData, EdgeData>();
    myGraph.setPid(pid / subpartPerPartition);
    ((SimpleSubGraph) myGraph).setSubPid(pid % subpartPerPartition);

    LOG.info("Reduce edges for graph: " + pid);
    while (iter.hasNext()) {
      ValueType val = iter.next();
      CombinedEdgeValueType evalue = val.edgeValue();
      myGraph.addEdges(evalue.sources(), evalue.targets(), evalue.edata());
    }

    // Switch to GLGraph by uncommenting the next line.
    // GLJsonFormatter formatter = new GLJsonFormatter();
    SimpleJsonFormatter formatter = new SimpleJsonFormatter();
    LOG.info("Write out graph " + pid + " with " + myGraph.numEdges()
        + " edges");
    graphOutput.writeAndClear(myGraph, formatter, out, reporter);

    LOG.info("Done reducing graph:" + pid + ".");
  }

  /**
   * Reduce a list of VertexRecordValues and the Vertex Data into a vertex
   * record.
   *
   * @param pid
   * @param iter
   * @param reporter
   */
  protected void reduceVertex(VidType vid, Iterator<ValueType> iter,
      Reporter reporter) {
    vrecord = new VertexRecord<VidType, VertexData>(vid);

    BitSet mirrors = new BitSet(numProcs);
    int inEdges = 0;
    int outEdges = 0;
      
    while (iter.hasNext()) {
      ValueType val = iter.next();

      CombinedVrecordValueType vrecordValue = val.vrecordValue();
      inEdges += vrecordValue.inEdges();
      outEdges += vrecordValue.outEdges();
      HashSet<Short> pids = vrecordValue.pids();
      Iterator<Short> piditer = pids.iterator();
      while (piditer.hasNext()) {
        mirrors.set(piditer.next());
      }

      vrecord.setMirrors(mirrors);
      vrecord.setInEdges(inEdges);
      vrecord.setOutEdges(outEdges);

      // merge vdata
      if (vrecordValue.hasVdata()) {
        if (vrecord.vdata() == null)
          vrecord.setVdata((VertexData) vrecordValue.vdata());
        else if (vrecord.vdata() instanceof Mergable) {
          ((Mergable) vrecord.vdata()).add(vrecordValue.vdata());
        }
      }
    }

    // Set owner
    Random generator = new Random();
    if (vrecord.numMirrors() == 0) {
      vrecord.setOwner((short) generator.nextInt(numProcs));
    } else {
      List<Short> mirrorsList = vrecord.mirrorList();
      vrecord.setOwner(mirrorsList.get(generator.nextInt(mirrorsList.size())));
      vrecord.removeMirror(vrecord.owner());
    }
  }

  int numProcs, subpartPerPartition;
  protected Graph<VidType, VertexData, EdgeData> myGraph;
  protected VertexRecord<VidType, VertexData> vrecord;
  protected GraphOutput graphOutput;
}

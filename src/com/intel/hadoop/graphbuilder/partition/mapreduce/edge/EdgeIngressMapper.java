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
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.graph.Edge;
import com.intel.hadoop.graphbuilder.graph.Vertex;
import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.IngressKeyType;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.IngressValueType;
import com.intel.hadoop.graphbuilder.partition.strategy.GreedyIngress;
import com.intel.hadoop.graphbuilder.partition.strategy.ConstrainedGreedyIngress;
import com.intel.hadoop.graphbuilder.partition.strategy.ConstrainedRandomIngress;
import com.intel.hadoop.graphbuilder.partition.strategy.ConstrainedPDSRandomIngress;
import com.intel.hadoop.graphbuilder.partition.strategy.Ingress;
import com.intel.hadoop.graphbuilder.partition.strategy.RandomIngress;

/**
 * 
 * This mapper class maps edge and vertex list into intermediate key, value
 * pairs. Because mapper takes in edge and vertex at the same time, "!" is used
 * as a special character in the beginning of each vertex data to distinguish it
 * from edge data.
 * <p>
 * For each edge e = (u,v,edata) it assigns its partition using the ingress
 * algorithm defined by the {@code JobConf}. After computing the partition id,
 * it generates 3 records:
 * <ul>
 * <li>EdgeType value: {@code (pid,  (u,v,edata))}</li>
 * <li>VertexType value: {@code (u, ( pid}, 0, 1))}</li>
 * <li>VertexType value: {@code (v, ( pid}, 1, 0))}</li>
 * </ul>
 * For each vertex v = (v, vdata) it generates a VertexType value:
 * {@code (v, vdata)}.
 * </p>
 * <p>
 * All EdgeType value with the same partition id is reduced into a local graph
 * partition. And all VertexType value with the same vertex id is reduced into a
 * vertex record.
 * </p>
 * 
 * @param <VidType>
 * @param <VertexData>
 * @param <EdgeData>
 * @param <KeyType>
 * @param <ValueType>
 */
public class EdgeIngressMapper<VidType extends WritableComparable<VidType>, VertexData extends Writable, EdgeData extends Writable, KeyType extends IngressKeyType<VidType>, ValueType extends IngressValueType<VidType, VertexData, EdgeData>>
    extends MapReduceBase implements
    Mapper<LongWritable, Text, KeyType, ValueType> {

  private static final Logger LOG = Logger.getLogger(EdgeIngressMapper.class);

  @SuppressWarnings("unchecked")
  @Override
  public void configure(JobConf job) {
    super.configure(job);
    this.keyClass = job.getMapOutputKeyClass();
    this.valClass = job.getMapOutputValueClass();
    numprocs = job.getInt("numProcs", 1);
    overpartition = job.getInt("overpartition", 1);

    String ingressMethod = job.get("ingress");
    if (ingressMethod.equals("greedy")) {
      this.ingress = new GreedyIngress<VidType>(numprocs);
    } else if (ingressMethod.equals("constrainedgreedy")) {
      this.ingress = new ConstrainedGreedyIngress<VidType>(numprocs);
    } else if (ingressMethod.equals("constrainedrandom")) {
      this.ingress = new ConstrainedRandomIngress<VidType>(numprocs);
    } else if (ingressMethod.equals("constrainedpdsrandom")) {
      this.ingress = new ConstrainedPDSRandomIngress<VidType>(numprocs);   
    } else if (ingressMethod.equals("random")) {
      this.ingress = new RandomIngress<VidType>(numprocs);
    }

    try {
      this.graphparser = (GraphParser) Class.forName(job.get("GraphParser"))
          .newInstance();
      this.vidparser = (FieldParser) Class.forName(job.get("VidParser"))
          .newInstance();
      this.vdataparser = (FieldParser) Class.forName(job.get("VdataParser"))
          .newInstance();
      this.edataparser = (FieldParser) Class.forName(job.get("EdataParser"))
          .newInstance();
      this.mapKey = (KeyType) keyClass.newInstance();
      this.mapValue = (ValueType) valClass.newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void map(LongWritable key, Text value,
      OutputCollector<KeyType, ValueType> out, Reporter reporter)
      throws IOException {

    String text = value.toString();
    if (text.startsWith("!")) {
      // Remove vertex escape character ! before parsing
      text = text.substring(1);
      if (graphparser.isVertexData(text))
        mapVertexInput(text, out, reporter);
    } else if (graphparser.isEdgeData(text)) {
      mapEdgeInput(text, out, reporter);
    } else {
      LOG.error("Fail to parse: " + value.toString());
    }
  }

  /**
   * Maps the edge input into an edge record and 2 vertex record.
   * 
   * @param value
   * @param out
   * @param reporter
   */
  protected void mapEdgeInput(String value,
      OutputCollector<KeyType, ValueType> out, Reporter reporter) {
    try {
      Edge<VidType, EdgeData> e = graphparser.parseEdge(value, vidparser,
          edataparser);
      short pid = ingress.computePid(e.source(), e.target());

      // overpartition edges and assign its quasi pid.
      Random r = new Random();
      short qid = (short) (overpartition * pid + r.nextInt(overpartition));
      LOG.error(mapKey.toString() + mapValue.toString());
      mapKey.set(qid, null, IngressKeyType.EDGEKEY);
      mapValue.initEdgeValue(qid, e.source(), e.target(), e.EdgeData());
      out.collect(mapKey, mapValue);

      // output source vertex record
      //mapKey.set(pid, e.source(), IngressKeyType.VERTEXKEY);
      //mapValue.initVrecValue(e.source(), pid, 0, 1);
      mapKey.set(qid, e.source(), IngressKeyType.VERTEXKEY);
      mapValue.initVrecValue(e.source(), qid, 0, 1);
      out.collect(mapKey, mapValue);

      // output target vertex record
      //mapKey.set(pid, e.target(), IngressKeyType.VERTEXKEY);
      //mapValue.initVrecValue(e.target(), pid, 1, 0);
      mapKey.set(qid, e.target(), IngressKeyType.VERTEXKEY);
      mapValue.initVrecValue(e.target(), qid, 1, 0);
      out.collect(mapKey, mapValue);

    } catch (Exception e1) {
      e1.printStackTrace();
    }
  }

  /**
   * Maps the vertex input into a vertex value.
   * 
   * @param value
   * @param out
   * @param reporter
   */
  protected void mapVertexInput(String value,
      OutputCollector<KeyType, ValueType> out, Reporter reporter) {
    try {
      Vertex<VidType, VertexData> v = graphparser.parseVertex(value, vidparser,
          vdataparser);
      // output vertex value map
      short pid = -1;
      mapKey.set(pid, v.vid(), IngressKeyType.VERTEXKEY);
      mapValue.initVrecValue(v.vid(), v.vdata());
      out.collect(mapKey, mapValue);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected GraphParser<VidType, VertexData, EdgeData> graphparser;
  protected FieldParser<VidType> vidparser;
  protected FieldParser<VertexData> vdataparser;
  protected FieldParser<EdgeData> edataparser;

  protected KeyType mapKey;
  protected ValueType mapValue;
  protected Ingress<VidType> ingress;
  protected Class keyClass;
  protected Class valClass;
  protected int numprocs;
  protected int overpartition;
}

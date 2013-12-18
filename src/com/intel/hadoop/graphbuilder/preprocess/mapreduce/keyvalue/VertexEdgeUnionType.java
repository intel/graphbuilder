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
package com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.intel.hadoop.graphbuilder.graph.Edge;
import com.intel.hadoop.graphbuilder.graph.Vertex;

/**
 * Abstract union type of {@code Vertex} and {@code Edge}. Used as intermediate
 * map output value to hold either a vertex or an edge.
 * 
 * @param <VidType>
 * @param <VertexData>
 * @param <EdgeData>
 */
public abstract class VertexEdgeUnionType<VidType extends WritableComparable<VidType>, VertexData extends Writable, EdgeData extends Writable>
    implements Writable {

  public static final boolean VERTEXVAL = false;
  public static final boolean EDGEVAL = true;;

  public abstract VidType createVid();

  public abstract VertexData createVdata();

  public abstract EdgeData createEdata();

  /**
   * Creates an empty value.
   */
  public VertexEdgeUnionType() {
    vertex = new Vertex<VidType, VertexData>();
    edge = new Edge<VidType, EdgeData>();
  }

  /**
   * Initialize the value.
   * 
   * @param flag
   * @param value
   */
  public void init(boolean flag, Object value) {
    this.flag = flag;
    if (flag == VERTEXVAL) {
      this.vertex = (Vertex<VidType, VertexData>) value;
      this.edge = null;
    } else {
      this.edge = (Edge<VidType, EdgeData>) value;
      this.vertex = null;
    }
  }

  /**
   * @return the type flag of the value.
   */
  public boolean flag() {
    return flag;
  }

  /**
   * @return the vertex value, used only when flag == VERTEXVAL.
   */
  public Vertex<VidType, VertexData> vertex() {
    return vertex;
  }

  /**
   * @return the vertex value, used only when flag == EDGEXVAL.
   */
  public Edge<VidType, EdgeData> edge() {
    return edge;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    flag = arg0.readBoolean();
    if (flag == VERTEXVAL) {
      VidType vid = createVid();
      vid.readFields(arg0);
      VertexData vdata = createVdata();
      vdata.readFields(arg0);
      vertex.set(vid, vdata);
    } else {
      VidType source = createVid();
      source.readFields(arg0);
      VidType target = createVid();
      target.readFields(arg0);
      EdgeData edata = createEdata();
      edata.readFields(arg0);
      edge.set(source, target, edata);
    }
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    arg0.writeBoolean(flag);
    if (flag == VERTEXVAL) {
      vertex.vid().write(arg0);
      vertex.vdata().write(arg0);
    } else {
      edge.source().write(arg0);
      edge.target().write(arg0);
      edge.EdgeData().write(arg0);
    }
  }

  @Override
  public String toString() {
    if (flag == VERTEXVAL)
      return vertex.toString();
    return edge.toString();
  }

  private boolean flag;
  private Vertex<VidType, VertexData> vertex;
  private Edge<VidType, EdgeData> edge;
}

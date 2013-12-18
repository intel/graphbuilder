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
package com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Intermediate value type of edge records and supports commutative and
 * associative add operation to merge with instances of the same type.
 * 
 */
public class CombinedEdgeValueType<VidType extends WritableComparable<VidType>, VertexData extends Writable, EdgeData extends Writable>
    implements Writable {
  public CombinedEdgeValueType(GraphTypeFactory factory) {
    this.factory = factory;
  }

  /**
   * Create an instance with one edge.
   * 
   * @param source
   * @param target
   * @param data
   */
  public CombinedEdgeValueType(VidType source, VidType target, EdgeData data,
      GraphTypeFactory factory) {
    sources = new ArrayList<VidType>(1);
    targets = new ArrayList<VidType>(1);
    edata = new ArrayList<EdgeData>(1);
    sources.add(source);
    targets.add(target);
    edata.add(data);
    this.factory = factory;
  }

  /**
   * Combines with other instance.
   *
   * @param other
   */
  public void add(CombinedEdgeValueType other) {
    if (this.size() == 0 && other.size() == 0)
      return;
    if (this.size() == 0) {
      this.sources = new ArrayList<VidType>(other.size());
      this.targets = new ArrayList<VidType>(other.size());
      this.edata = new ArrayList<EdgeData>(other.size());
    }
    this.sources.addAll(other.sources);
    this.targets.addAll(other.targets);
    this.edata.addAll(other.edata);
  }

  /**
   * @return number of edges in this value.
   */
  public int size() {
    return sources == null ? 0 : sources.size();
  }

  /**
   * Clear the edges in the value.
   */
  public void clear() {
    sources.clear();
    targets.clear();
    edata.clear();
  }

  /**
   * @return the source ids of the edges.
   */
  public List<VidType> sources() {
    return sources;
  }

  /**
   * @return the target ids of the edges.
   */
  public List<VidType> targets() {
    return targets;
  }

  /**
   * @return edge data of the edges.
   */
  public List<EdgeData> edata() {
    return edata;
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    pid = in.readShort();
    int size = in.readInt();
    if (sources == null)
      sources = new ArrayList<VidType>(size);
    if (targets == null)
      targets = new ArrayList<VidType>(size);
    if (edata == null)
      edata = new ArrayList<EdgeData>(size);

    for (int i = 0; i < size; ++i) {
      VidType source = factory.createVid();
      source.readFields(in);
      sources.add(source);
    }
    for (int i = 0; i < size; ++i) {
      VidType target = factory.createVid();
      target.readFields(in);
      targets.add(target);
    }
    for (int i = 0; i < size; ++i) {
      EdgeData data = factory.createEdata();
      data.readFields(in);
      edata.add(data);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(pid);
    out.writeInt(size());
    for (int i = 0; i < size(); ++i) {
      sources.get(i).write(out);
    }
    for (int i = 0; i < size(); ++i) {
      targets.get(i).write(out);
    }
    for (int i = 0; i < size(); ++i) {
      edata.get(i).write(out);
    }
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("size: " + sources.size() + "[");
    for (int i = 0; i < sources.size(); ++i) {
      sb.append("(" + sources.get(i) + ", " + targets.get(i) + ", " + edata.get(i) + ") ");
    }
    return sb.toString();
  }

  short pid;
  private ArrayList<VidType> sources;
  private ArrayList<VidType> targets;
  private ArrayList<EdgeData> edata;
  private GraphTypeFactory<VidType, VertexData, EdgeData> factory;
}

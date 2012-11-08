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
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.intel.hadoop.graphbuilder.types.Mergable;

/**
 * Intermediate value type of vertex records and supports commutative and
 * associative add operation through additvity of inEdges, outEdges and union of
 * the mirror set.
 * 
 */
public class CombinedVrecordValueType<VidType extends WritableComparable<VidType>, VertexData extends Writable, EdgeData extends Writable>
    implements Writable {
  /**
   * Create an empty vertex record.
   */
  public CombinedVrecordValueType(GraphTypeFactory factory) {
    this.vid = null;
    this.vdata = null;
    pids = new HashSet<Short>();
    this.hasvdata = false;
    this.factory = factory;
  }

  /**
   * Creates a vertex record with vid, pid, inEdges and ouEdges. Vertex data is
   * empty.
   * 
   * @param vid
   * @param pid
   * @param inEdges
   * @param outEdges
   */
  public CombinedVrecordValueType(VidType vid, short pid, int inEdges,
      int outEdges, GraphTypeFactory factory) {
    this.vid = vid;
    pids = new HashSet<Short>();
    pids.add(pid);
    this.inEdges = inEdges;
    this.outEdges = outEdges;
    this.hasvdata = false;
    this.factory = factory;
  }

  /**
   * Creates a vertex record with vid and vdata. Other fields are left empty.
   * 
   * @param vid
   * @param vdata
   */
  public CombinedVrecordValueType(VidType vid, VertexData vdata) {
    this.vid = vid;
    this.vdata = vdata;
    this.inEdges = 0;
    this.outEdges = 0;
    this.pids = new HashSet<Short>();
    this.hasvdata = true;
  }

  /**
   * Combine with other VertexRecord value. They must have the same vid.
   * 
   * @param other
   */
  public void add(CombinedVrecordValueType<VidType, VertexData, EdgeData> other) {
    if (this.vid == null) {
      this.vid = other.vid;
    }

    if (!this.vid.equals(other.vid)) {
      // fatal error
    } else {
      this.inEdges += other.inEdges;
      this.outEdges += other.outEdges;

      // merge pids
      Iterator<Short> iter = other.pids.iterator();
      while (iter.hasNext()) {
        pids.add(iter.next());
      }

      // merge vdata
      if (this.hasvdata) {
        if (other.hasvdata && vdata instanceof Mergable) {
          ((Mergable) vdata).add(other.vdata);
        }
      } else {
        if (other.hasvdata) {
          this.vdata = other.vdata;
          this.hasvdata = true;
        }
      }
    }
  }

  /**
   * @return vertex id.
   */
  public VidType vid() {
    return vid;
  }

  /**
   * @return number of incoming edges.
   */
  public int inEdges() {
    return inEdges;
  }

  /**
   * @return number of outgoing edges.
   */
  public int outEdges() {
    return outEdges;
  }

  /**
   * @return true if this value has vertex data.
   */
  public boolean hasVdata() {
    return hasvdata;
  }

  /**
   * @return partition ids that contains the mirror of this vertex.
   */
  public HashSet<Short> pids() {
    return pids;
  }

  /**
   * @return vertex data.
   */
  public VertexData vdata() {
    return vdata;
  }

  /**
   * Clear this value.
   */
  public void clear() {
    pids.clear();
    vdata = null;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    vid = factory.createVid();
    vid.readFields(in);
    hasvdata = in.readBoolean();
    if (hasvdata) {
      vdata = factory.createVdata();
      vdata.readFields(in);
    }
    int numMirrors = in.readShort();
    for (int i = 0; i < numMirrors; ++i) {
      short mirror = in.readShort();
      pids.add(mirror);
    }
    inEdges = in.readInt();
    outEdges = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    vid.write(out);
    out.writeBoolean(hasvdata);
    if (hasvdata)
      vdata.write(out);
    out.writeShort((short) pids.size());
    Iterator<Short> iter = pids.iterator();
    while (iter.hasNext())
      out.writeShort(iter.next());
    out.writeInt(inEdges);
    out.writeInt(outEdges);
  }

  private VidType vid;
  private VertexData vdata;
  private boolean hasvdata;
  private int inEdges;
  private int outEdges;
  private HashSet<Short> pids;
  private GraphTypeFactory<VidType, VertexData, EdgeData> factory;
}
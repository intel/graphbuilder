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
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * An abstract class for map value templated with VidType, VertexData, and
 * EdgeData. MapValue is a "union" type EdgeRecordType, and VertexRecordType.
 * Both sub types are combinable with the same subtype.
 * 
 * @param <VidType>
 */
public abstract class IngressValueType<VidType extends WritableComparable<VidType>, VertexData extends Writable, EdgeData extends Writable>
    implements Writable {
  private static final Logger LOG = Logger.getLogger(IngressValueType.class);

  /**
   * Enums of the map key types. EDGEKEY represents an intermediate edge record:
   * (pid, (source, target, data)). VERTEXKEY is for a vertex record (meta
   * information) without vdata: (vid, mirrors, vdata=empty). VERTEXVALKEY is
   * for a vertex data: (vid, vdata). The reducer calls different reduce methods
   * based on the key type.
   */
  public static final short EDGEVALUE = 0;
  public static final short VRECORDVALUE = 1;

  public abstract GraphTypeFactory getGraphTypeFactory();

  public short flag() {
    return flag;
  }

  /**
   * Reduce a list of values. Value
   * 
   * @param flag
   * @param iter
   */
  public void reduce(short flag,
      Iterator<IngressValueType<VidType, VertexData, EdgeData>> iter) {
    if (flag == IngressKeyType.EDGEKEY) {
      edgeValue = new CombinedEdgeValueType(getGraphTypeFactory());
      while (iter.hasNext()) {
        IngressValueType<VidType, VertexData, EdgeData> next = iter.next();
        edgeValue.add(next.edgeValue());
        CombinedEdgeValueType eval = next.edgeValue();
      }
      this.flag = EDGEVALUE;
    } else if (flag == IngressKeyType.VERTEXKEY) {
      vrecordValue = new CombinedVrecordValueType(getGraphTypeFactory());
      while (iter.hasNext()) {
        IngressValueType<VidType, VertexData, EdgeData> next = iter.next();
        vrecordValue.add(next.vrecordValue());
      }
      this.flag = VRECORDVALUE;
    } else {
      LOG.error("Unknown Ingress Key Type: " + flag);
    }
  }

  /**
   * Initialize the EdgeRecord value.
   * 
   * @param pid
   * @param source
   * @param target
   * @param edata
   */
  public void initEdgeValue(short pid, VidType source, VidType target,
      EdgeData edata) {
    this.flag = EDGEVALUE;
    this.edgeValue = new CombinedEdgeValueType(source, target, edata,
        getGraphTypeFactory());
    this.vrecordValue = null;
  }

  /**
   * Initialize as a VertexRecord value without vertex data.
   * 
   * @param vid
   * @param pid
   * @param inEdges
   * @param outEdges
   */
  public void initVrecValue(VidType vid, short pid, int inEdges, int outEdges) {
    this.flag = VRECORDVALUE;
    this.vrecordValue = new CombinedVrecordValueType(vid, pid, inEdges,
        outEdges, getGraphTypeFactory());
    this.edgeValue = null;

  }

  /**
   * Initialize as a VertexRecord value with vdata.
   * 
   * @param vid
   * @param vdata
   */
  public void initVrecValue(VidType vid, VertexData vdata) {
    this.flag = VRECORDVALUE;
    this.vrecordValue = new CombinedVrecordValueType(vid, vdata);
    this.edgeValue = null;
  }

  /**
   * Clear the edgevalue and vrecordvalue.
   */
  public void clear() {
    if (flag == EDGEVALUE) {
      edgeValue.clear();
    } else if (flag == VRECORDVALUE) {
      vrecordValue.clear();
    } else {

    }
    edgeValue = null;
    vrecordValue = null;
  }

  /**
   * @return the edgevalue, use only when flag == EDGEVALUE.
   */
  public CombinedEdgeValueType edgeValue() {
    return edgeValue;
  }

  /**
   * @return the edgevalue, use only when flag == VRECORDVALUE.
   */
  public CombinedVrecordValueType vrecordValue() {
    return vrecordValue;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    flag = in.readShort();
    if (flag == EDGEVALUE) {
      edgeValue = new CombinedEdgeValueType(getGraphTypeFactory());
      edgeValue.readFields(in);
    } else if (flag == VRECORDVALUE) {
      vrecordValue = new CombinedVrecordValueType(getGraphTypeFactory());
      vrecordValue.readFields(in);
    } else {

    }
  }

  @Override
  public void write(DataOutput out) throws IOException {

    out.writeShort(flag);
    if (flag == EDGEVALUE) {
      edgeValue.write(out);
    } else if (flag == VRECORDVALUE) {
      vrecordValue.write(out);
    } else {
    }
  }

  protected CombinedEdgeValueType edgeValue;
  protected CombinedVrecordValueType vrecordValue;
  protected GraphTypeFactory<VidType, VertexData, EdgeData> factory;
  protected short flag;
}

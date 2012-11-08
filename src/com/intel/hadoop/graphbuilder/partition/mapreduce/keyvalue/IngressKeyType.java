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

import org.apache.hadoop.io.WritableComparable;

/**
 * Abstract MapKey for edge ingress templated with VidType. This key is a
 * "union" type of two types: EdgeKey and VertexKey. The reducer calls different
 * reduce methods based on the key type.
 * 
 * @param <VidType>
 */
public abstract class IngressKeyType<VidType extends WritableComparable<VidType>>
    implements WritableComparable<IngressKeyType<VidType>> {

  /**
   * Enums of the map key types. EDGEKEY represents an intermediate edge record:
   * (pid, (source, target, data)). VERTEXKEY is for a {@code VertexRecord}
   * (vid, mirrors, inEdges, outEdges, vdata=empty). The reducer calls different
   * reduce methods based on the key type.
   */
  public static final short EDGEKEY = 0;
  public static final short VERTEXKEY = 1;

  public abstract VidType createVid();

  public IngressKeyType() {
    vid = this.createVid();
  }

  /**
   * 
   * @param pid
   * @param vid
   * @param flag
   */
  public void set(short pid, VidType vid, short flag) {
    this.flag = flag;
    this.vid = vid;
    this.pid = pid;
  }

  /**
   * @return the type flag
   */
  public short flag() {
    return flag;
  }

  /**
   * @param flag
   *          the new type flag
   */
  public void setFlag(short flag) {
    this.flag = flag;
  }

  /**
   * @return the vertex id, for VertexKey only.
   */
  public VidType vid() {
    return vid;
  }

  /**
   * @return the partition id, for EdgeKey only.
   */
  public short pid() {
    return pid;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    flag = in.readShort();
    if (flag == EDGEKEY) {
      pid = in.readShort();
    } else {
      vid.readFields(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(flag);
    if (flag == EDGEKEY) {
      out.writeShort(pid);
    } else {
      vid.write(out);
    }
  }

  @Override
  public String toString() {
    if (this.flag == EDGEKEY) {
      return String.valueOf((int) pid);
    } else {
      return vid.toString();
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IngressKeyType) {
      if (((IngressKeyType) obj).flag == this.flag) {
        return flag == EDGEKEY ? (((IngressKeyType) obj).pid == this.pid)
            : ((IngressKeyType) obj).vid.equals(this.vid);
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return flag == EDGEKEY ? (int) pid : vid.hashCode();
  }

  @Override
  public int compareTo(IngressKeyType<VidType> other) {
    if (this.flag == other.flag) {
      if (this.flag == EDGEKEY) {
        return ((Short) pid).compareTo(other.pid);
      } else {
        return vid.compareTo((VidType) other.vid);
      }
    } else {
      return ((Short) (this.flag)).compareTo(other.flag);
    }
  }

  /** Type of this key. */
  protected short flag;

  /** Vertex id of this record. Used in VERTEXKEY and VERTEXVALKEY. */
  protected VidType vid;

  /** Partition id of this record. Used in EDGEKEY. */
  protected short pid;
}

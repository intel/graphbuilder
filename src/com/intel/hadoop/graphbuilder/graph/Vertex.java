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
package com.intel.hadoop.graphbuilder.graph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Represents a vertex object with id and vertex data.
 * 
 * @param <VidType>
 *          the type of vertex id.
 * @param <VertexData>
 *          the type of vertex data.
 */
public class Vertex<VidType extends WritableComparable<VidType>, VertexData extends Writable> {

  /** Default constructor. Creates an empty vertex. */
  public Vertex() {
  }

  /**
   * Creates a vertex with given vertex id and vertex data.
   * 
   * @param vid
   * @param vdata
   */
  public Vertex(VidType vid, VertexData vdata) {
    this.vid = vid;
    this.vdata = vdata;
  }

  /** Returns the id of the vertex. */
  public VidType vid() {
    return vid;
  }

  /** Returns the data of the vertex. */
  public VertexData vdata() {
    return vdata;
  }

  /**
   * Set the vertex fields with given vertex id and data, overwriting existing
   * fields.
   * 
   * @param vid
   *          the id of the new vertex.
   * @param vdata
   *          the data of the new vertex.
   */
  public void set(VidType vid, VertexData vdata) {
    this.vid = vid;
    this.vdata = vdata;
  }

  @Override
  public final boolean equals(Object obj) {
    if (obj instanceof Vertex) {
      Vertex other = (Vertex) obj;
      return (vid.equals(other.vid) && vdata.equals(other.vdata));
    }
    return false;
  }

  @Override
  public final int hashCode() {
    return vid.hashCode();
  }

  @Override
  public final String toString() {
    return vid.toString() + "\t" + vdata.toString();
  }

  private VidType vid;
  private VertexData vdata;

}

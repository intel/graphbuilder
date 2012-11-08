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

import com.intel.hadoop.graphbuilder.util.HashUtil;

/**
 * Represents an Edge object with source, target vertex id and edge data.
 * 
 * @param <VidType>
 *          the type of vertex id.
 * @param <EdgeData>
 *          the type of edge data.
 */
public class Edge<VidType extends WritableComparable<VidType>, EdgeData extends Writable> {

  /** Default constructor. Creates an empty edge. */
  public Edge() {
  }

  /**
   * Creates an edge with given source, target and edge data.
   * 
   * @param source
   * @param target
   * @param edata
   */
  public Edge(VidType source, VidType target, EdgeData edata) {
    this.source = source;
    this.target = target;
    this.edata = edata;
  }

  /**
   * @return edge data.
   */
  public EdgeData EdgeData() {
    return edata;
  }

  /**
   * @return source vertex id.
   */
  public VidType source() {
    return source;
  }

  /**
   * @return target vertex id.
   */
  public VidType target() {
    return target;
  }

  /**
   * Set an edge with given source target ids and edge data, overwriting
   * exisiting fields.
   * 
   * @param source
   * @param target
   * @param edata
   */
  public void set(VidType source, VidType target, EdgeData edata) {
    this.source = source;
    this.target = target;
    this.edata = edata;
  }

  @Override
  public final boolean equals(Object obj) {
    if (obj instanceof Edge) {
      Edge other = (Edge) obj;
      return (source.equals(other.source) && target.equals(other.target) && edata
          .equals(other.edata));
    } else {
      return false;
    }
  }

  @Override
  public final int hashCode() {
    return HashUtil.hashpair(source, target);
  }

  @Override
  public final String toString() {
    return source.toString() + "\t" + target.toString() + "\t"
        + edata.toString();
  }

  private VidType source;
  private VidType target;
  private EdgeData edata;
}

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
package com.intel.hadoop.graphbuilder.parser;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.intel.hadoop.graphbuilder.graph.Edge;
import com.intel.hadoop.graphbuilder.graph.Vertex;

/**
 * A interface for parsing vertices and edges of a graph. This interface
 * provides functionality of taking in a String and return an Edge or Vertex
 * Object associated with specific Vid, Vdata and Edata types.
 * 
 * @param <VidType>
 * @param <VertexData>
 * @param <EdgeData>
 */
public interface GraphParser<VidType extends WritableComparable<VidType>, VertexData extends Writable, EdgeData extends Writable> {
  /**
   * @param text
   * @return true if text is a edge data.
   */
  boolean isEdgeData(String text);

  /**
   * @param text
   * @return true if text is a vertex data.
   */
  boolean isVertexData(String text);

  /**
   * @param text
   * @param vidparser
   *          FieldParser for vertex id.
   * @param edataparser
   *          FieldParser for edge data.
   * @return {@code Edge} object with parsed source, target ids and data.
   */
  Edge<VidType, EdgeData> parseEdge(String text,
      FieldParser<VidType> vidparser, FieldParser<EdgeData> edataparser);

  /**
   * @param text
   * @param vidparser
   *          FieldParser for vertex id.
   * @param vdataparser
   *          FieldParser for edge data.
   * @return {@code Vertex} object with parsed vertex id and data.
   */
  Vertex<VidType, VertexData> parseVertex(String text,
      FieldParser<VidType> vidparser, FieldParser<VertexData> vdataparser);
}

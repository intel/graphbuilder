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
package com.intel.hadoop.graphbuilder.preprocess.inputformat;

import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;

import com.intel.hadoop.graphbuilder.graph.Edge;
import com.intel.hadoop.graphbuilder.graph.Vertex;

/**
 * Tokenize the input provided by {@code InputFormat} into a list of
 * {@code Vertex} and and a list of {@code Edge} objects. This should be the
 * first step to implement along with the design of the {@code InputFormat} of
 * the raw input.
 * 
 * @see InputFormat
 * 
 * @param <VidType>
 * @param <VertexData>
 * @param <EdgeData>
 */
public interface GraphTokenizer<VidType extends WritableComparable<VidType>, VertexData extends Writable, EdgeData extends Writable> {

  /**
   * Configure the tokenizer from JobConf.
   * 
   * @param job
   */
  void configure(JobConf job);

  /**
   * Parse the input string and filter into internal vertex and edge fields.
   * 
   * @param s
   */
  void parse(String s);

  /**
   * @return a list of {@code Vertex} extracted from the input.
   */
  Iterator<Vertex<VidType, VertexData>> getVertices();

  /**
   * @return a list of {@code Edge} extracted from the input.
   */
  Iterator<Edge<VidType, EdgeData>> getEdges();

  /**
   * @return Class of the VidType. Used for type safety in the high level.
   */
  Class vidClass();

  /**
   * @return Class of the VertexData. Used for type safety in the high level.
   */
  Class vdataClass();

  /**
   * @return Class of the EdgeData. Used for type safety in the high level.
   */
  Class edataClass();
}

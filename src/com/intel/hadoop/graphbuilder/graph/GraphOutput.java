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

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Controls the high level output logic of a graph, including initializing,
 * finalizing the Hadoop IO objects, and configuring the output directory. The
 * output only writes out the local part of the graph (the edges) and does not
 * write the vertex record information. This is because the ingress of edges and
 * vertices are separate processes.
 * 
 */
public interface GraphOutput {
  /**
   * Set the {@code OutputFormat} in the {@code JobConf}.
   * 
   * @param conf
   */
  void init(JobConf conf);

  /**
   * Configure this GraphOutput using {@code JobConf}.
   * 
   * @param conf
   */
  void configure(JobConf conf);

  /**
   * Write out the graph to the {@code OutputCollector}.
   * 
   * @param g
   *          the graph to output.
   * @param formatter
   *          formatter for the graph string representation.
   * @param out
   * @param reporter
   * @throws Exception
   */
  void write(Graph g, EdgeFormatter formatter, OutputCollector out,
      Reporter reporter) throws Exception;

  /**
   * Write out the graph and clearing the fields written to free up memory.
   * 
   * @param g
   *          the graph to output.
   * @param formatter
   *          formatter of the graph string representation.
   * @param out
   * @param reporter
   * @throws IOException
   */
  void writeAndClear(Graph g, EdgeFormatter formatter, OutputCollector out,
      Reporter reporter) throws Exception;

  /**
   * Close the output.
   * 
   * @throws IOException
   * */
  void close() throws IOException;
}

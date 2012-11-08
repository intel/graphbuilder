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
package com.intel.hadoop.graphbuilder.graph.simplegraph;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.graph.Graph;
import com.intel.hadoop.graphbuilder.graph.EdgeFormatter;
import com.intel.hadoop.graphbuilder.graph.GraphOutput;
import com.intel.hadoop.graphbuilder.io.MultiDirOutputFormat;

/**
 * Controls the output of a {@code SimpleGraph} or a {@code SimpleSubGraph}. It
 * outputs the graph into 3 parts: edge data, adjacency list and a metafile
 * sotring the number of edges in the partition or subpartition. The output
 * directory is organized as follows:
 * <ul>
 * <li>Edge data list: $prefix/partition{$i}/subpart{$j}/edata</li>
 * <li>Metafile: $prefix/partition{$i}/subpart{$j}/meta</li>
 * <li>Graph structure: $prefix/partition{$i}/subpart{$j}/edgelist</li>
 * </ul>
 * The actual output of each part is controlled by the {@code GraphFormatter}.
 * 
 */

public class SimpleGraphOutput implements GraphOutput {
  private static final Logger LOG = Logger.getLogger(SimpleGraphOutput.class);

  public SimpleGraphOutput() {
  }

  @Override
  public void init(JobConf conf) {
    conf.setOutputFormat(MultiDirOutputFormat.class);
  }

  @Override
  public void configure(JobConf conf) {
  }

  @Override
  public void write(Graph g, EdgeFormatter formatter, OutputCollector out,
      Reporter reporter) throws IOException {
    int pid = g.pid();
    int subpid = -1;
    if (g instanceof SimpleSubGraph) {
      subpid = ((SimpleSubGraph) g).subpid();
    }
    String basedir = subpid >= 0 ? "partition" + pid + "/subpart" + subpid
        : "partition" + pid;

    /* Output edge data. */
    LOG.debug("Collecting edata: " + pid);
    String emetaout = basedir + " meta";
    out.collect(new Text(emetaout), new Text("{\"numEdges\":" + g.numEdges()
        + "}"));

    StringWriter edataWriter = formatter.edataWriter((SimpleGraph) g);
    if (clearAfterWrite)
      ((SimpleGraph) g).clearEdataList();
    String edataout = basedir + " edata";
    out.collect(new Text(edataout), new Text(edataWriter.toString()));
    edataWriter.close();
    LOG.debug("Done collecting edata: " + pid);

    /* Output the graph structure. */
    LOG.debug("Collecting graph structure: " + pid);
    StringWriter structureWriter = formatter.structWriter(g);
    String structout = basedir + " edgelist";
    out.collect(new Text(structout), new Text(structureWriter.toString()));
    if (clearAfterWrite)
      g.clear();
    structureWriter.close();
    LOG.info("Done collecting graph structure: " + pid);
  }

  @Override
  public void writeAndClear(Graph g, EdgeFormatter formatter,
      OutputCollector out, Reporter reporter) throws IOException {
    clearAfterWrite = true;
    write(g, formatter, out, reporter);
  }

  @Override
  public void close() throws IOException {
  }

  boolean clearAfterWrite = false;
}

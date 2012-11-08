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
package com.intel.hadoop.graphbuilder.graph.glgraph;

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
 * Controls the output of a GLGraph. It outputs the graph into three parts:
 * {@code vid2lvid} map, edge data, and (CSC,CSR) adjacency structure. The
 * output directory is organized as follows:
 * <ul>
 * <li>Map vid -> lvid: $prefix/partition{$i}/vid2lvid/</li>
 * <li>Edge data list: $prefix/partition{$i}/edata/</li>
 * <li>Graph structure: $prefix/partition{$i}/structure</li>
 * </ul>
 * The actual output of each part is controlled by the {@code GraphFormatter}.
 * 
 */
public class GLGraphOutput implements GraphOutput {
  private static final Logger LOG = Logger.getLogger(GLGraphOutput.class);

  /** Default constructor. */
  public GLGraphOutput() {
  }

  @Override
  public final void init(JobConf conf) {
    conf.setOutputFormat(MultiDirOutputFormat.class);
  }

  @Override
  public void configure(JobConf conf) {
  }

  @Override
  public final void write(Graph g, EdgeFormatter formatter,
      OutputCollector out, Reporter reporter) throws Exception {

    int pid = g.pid();

    /* Output vid2lvidmap. */
    LOG.info("Collecting vid2lvid: " + pid);
    StringWriter vid2lvidWriter = ((GLJsonFormatter) formatter)
        .vid2lvidWriter((GLGraph) g);
    if (clearAfterWrite)
      ((GLGraph) g).vid2lvid().clear();

    out.collect(new Text("partition" + pid + "/vid2lvid"), new Text(
        vid2lvidWriter.toString()));
    vid2lvidWriter.close();
    LOG.info("Done collecting vid2lvid: " + pid);

    /* Graph finalize. */
    LOG.info("Finalizing graph: " + pid);
    g.finalize();
    LOG.info("Done finalizing finished: " + pid);

    /* Output edge data. */
    LOG.info("Collecting edata: " + pid);
    StringWriter edataWriter = ((GLJsonFormatter) formatter)
        .edataWriter((GLGraph) g);
    if (clearAfterWrite)
      ((GLGraph) g).edatalist().clear();

    out.collect(new Text("partition" + pid + "/edata"),
        new Text(edataWriter.toString()));
    edataWriter.close();
    LOG.info("Done collecting edata: " + pid);

    /* Output graph structure. */
    LOG.info("Collecting graph structure: " + pid);
    StringWriter structureWriter = formatter.structWriter(g);

    out.collect(new Text("partition" + pid + "/structure"), new Text(
        structureWriter.toString()));
    if (clearAfterWrite)
      g.clear();
    structureWriter.close();
    LOG.info("Done collecting graph structure: " + pid);
  }

  @Override
  public final void writeAndClear(Graph g, EdgeFormatter formatter,
      OutputCollector out, Reporter reporter) throws Exception {
    clearAfterWrite = true;
    write(g, formatter, out, reporter);
  }

  @Override
  public void close() throws IOException {
  }

  private boolean clearAfterWrite = false;
}

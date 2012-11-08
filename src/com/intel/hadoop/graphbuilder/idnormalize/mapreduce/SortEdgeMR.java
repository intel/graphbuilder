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
package com.intel.hadoop.graphbuilder.idnormalize.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.graph.Edge;
import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;

/**
 * This class partition the edge list input by the hash of the source vertex.
 * 
 */
public class SortEdgeMR {
  private static final Logger LOG = Logger.getLogger(SortEdgeMR.class);

  /**
   * This mapper class maps each edge into (h(edge.source), edge). The hash
   * function depends on "numChunks" passed through the {@code JobConf}.
   * 
   * @author Haijie Gu
   */
  public static class SortEdgeMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, IntWritable, Text> {

    public void configure(JobConf conf) {
      super.configure(conf);
      numChunks = conf.getInt("numChunks", 256);
      try {
        this.graphparser = (GraphParser) Class.forName(conf.get("GraphParser"))
            .newInstance();
        this.vidparser = (FieldParser) Class.forName(conf.get("VidParser"))
            .newInstance();
        this.edataparser = (FieldParser) Class.forName(conf.get("EdataParser"))
            .newInstance();
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void map(LongWritable key, Text val,
        OutputCollector<IntWritable, Text> out, Reporter arg3)
        throws IOException {
      Edge e = graphparser.parseEdge(val.toString(), vidparser, edataparser);
      int hash = e.source().hashCode() % numChunks;
      if (hash < 0)
        hash += numChunks;
      out.collect(new IntWritable(hash), val);
    }

    private int numChunks;
    private GraphParser graphparser;
    private FieldParser vidparser;
    private FieldParser edataparser;
  }

  /**
   * This reducer class takes the input (hashval, edge) from mapper and outputs
   * edge directly.
   * 
   * @author Haijie Gu
   */
  public static class SortEdgeReducer extends MapReduceBase implements
      Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterator<Text> iter,
        OutputCollector<IntWritable, Text> out, Reporter reporter)
        throws IOException {
      while (iter.hasNext()) {
        out.collect(null, iter.next());
      }
    }
  }

  public SortEdgeMR(int numChunks, GraphParser graphparser,
      FieldParser vidparser, FieldParser edataparser) {
    this.numChunks = numChunks;
    this.graphparser = graphparser;
    this.vidparser = vidparser;
    this.edataparser = edataparser;
  }

  public void run(String inputpath, String outputpath) throws IOException {

    JobConf conf = new JobConf(SortEdgeMR.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(SortEdgeMapper.class);
    conf.setReducerClass(SortEdgeReducer.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setInt("numChunks", numChunks);
    conf.set("GraphParser", graphparser.getClass().getName());
    conf.set("VidParser", vidparser.getClass().getName());
    conf.set("EdataParser", edataparser.getClass().getName());

    FileInputFormat.setInputPaths(conf, new Path(inputpath));
    FileOutputFormat.setOutputPath(conf, new Path(outputpath));

    LOG.info("==== Job: Partition the input edges by hash(sourceid) =========");
    LOG.info("Input = " + inputpath);
    LOG.info("Output = " + outputpath);
    LOG.debug("numChunks = " + numChunks);
    LOG.debug("GraphParser = " + graphparser.getClass().getName());
    LOG.debug("VidParser = " + vidparser.getClass().getName());
    LOG.debug("EdataParser = " + edataparser.getClass().getName());
    LOG.info("===============================================================");

    JobClient.runJob(conf);
    LOG.info("=================== Done ====================================\n");
  }

  private int numChunks;
  private GraphParser graphparser;
  private FieldParser vidparser;
  private FieldParser edataparser;

}

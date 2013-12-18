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
package com.intel.hadoop.graphbuilder.preprocess.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;
import com.intel.hadoop.graphbuilder.preprocess.functional.Functional;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.PairListType;

/**
 * This MapReduce Class applies user defined "Reduce" and "Apply" functional on
 * edges that share the same source or target vertex, determined by the
 * {@code reduceEndPoint} parameter. The Reduce functional computes a sufficient
 * statistics on a list of edges, and the Apply functional applies it back to
 * each edge in the list.
 * <p>
 * For example, the tf*idf {@link http://en.wikipedia.org/wiki/Tf*idf}
 * transformation can be done with two passes on a doc-word count graph.
 * </p>
 * <p>
 * First pass computes the term frequency (tf):
 * <ul>
 * <li>reduceEndPoint = SOURCE.</li>
 * <li>Reduce: sum x y -> x + y.</li>
 * <li>Apply: divide x z -> x / z.</li>
 * </p>
 * <p>
 * Second pass computes the final tfidf:
 * <ul>
 * <li>reduceEndPoint = TARGET.</li>
 * <li>Reduce: count x y -> y + 1.</li>
 * <li>Apply: tfidf tf doccount -> tf * log_10 (TOTAL_DOC / doccount).</li>
 * </ul>
 * </p>
 * <p>
 * Input directory: list of edges. Output directory: $outputdir contains list of
 * transformed edges.
 * </p>
 * 
 * @see TransformToTFIDF
 */
public class EdgeTransformMR {
  private static final Logger LOG = Logger.getLogger(EdgeTransformMR.class);
  public static final boolean SOURCE = false;
  public static final boolean TARGET = true;

  /**
   * Create a EdgeTransform Job with {@code reduceEndPoint} and parsers.
   * 
   * @param reduceEndPoint
   *          {SOURCE, TARGET} the edge end point to reduce on.
   * @param graphparser
   * @param vidparser
   * @param edataparser
   */
  public EdgeTransformMR(boolean reduceEndPoint, GraphParser graphparser,
      FieldParser vidparser, FieldParser edataparser) {
    this.reduceEndPoint = reduceEndPoint;
    this.graphparser = graphparser;
    this.vidparser = vidparser;
    this.edataparser = edataparser;
    conf = new JobConf(CreateGraphMR.class);
  }

  /**
   * Set the reduce and apply function.
   * 
   * @param reducefunc
   * @param applyfunc
   */
  public void setFunctionClass(Class reducefunc, Class applyfunc) {
    try {
      this.reducefunc = (Functional) reducefunc.newInstance();
      this.applyfunc = (Functional) applyfunc.newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  /**
   * Set the intermediate key value class.
   * 
   * @param keyClass
   * @param valClass
   */
  public void setKeyValueClass(Class keyClass, Class valClass) {
    try {
      this.mapkeytype = keyClass.newInstance();
      this.mapvaltype = (PairListType) valClass.newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  /**
   * Set the user defined options.
   * 
   * @param userOpts
   *          a Map of option key value pairs.
   */
  public void setUserOptions(HashMap<String, String> userOpts) {
    Set<String> s = userOpts.keySet();
    for (String key : s)
      conf.set(key, userOpts.get(key.toString()));
  }

  /**
   * @return JobConf of the current job.
   */
  public JobConf getConf() {
    return conf;
  }

  public void run(String inputpath, String outputpath) throws IOException {

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setMapperClass(EdgeTransformMapper.class);
    //conf.setCombinerClass(EdgeTransformCombiner.class);
    conf.setReducerClass(EdgeTransformReducer.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.set("ReduceFunc", reducefunc.getClass().getName());
    conf.set("ApplyFunc", applyfunc.getClass().getName());
    conf.setBoolean("reduceEndPoint", reduceEndPoint);
    conf.set("GraphParser", graphparser.getClass().getName());
    conf.set("VidParser", vidparser.getClass().getName());
    conf.set("EdataParser", edataparser.getClass().getName());

    FileInputFormat.setInputPaths(conf, new Path(inputpath));
    FileOutputFormat.setOutputPath(conf, new Path(outputpath));

    LOG.info("============== Job: Data Transformation on Edges ==========");
    LOG.info("Input = " + inputpath);
    LOG.info("Output = " + outputpath);
    LOG.info("reducefunc = " + reducefunc.getClass().getName());
    LOG.info("applyfunc = " + applyfunc.getClass().getName());
    if (reduceEndPoint == SOURCE)
      LOG.info("Reduce on source");
    else
      LOG.info("Reduce on target");

    LOG.info("===========================================================");
    JobClient.runJob(conf);
    LOG.info("======================== Done ============================\n");
  }


  private Functional reducefunc;
  private Functional applyfunc;
  private Object mapkeytype;
  private PairListType mapvaltype;

  private GraphParser graphparser;
  private FieldParser vidparser;
  private FieldParser edataparser;
  private boolean reduceEndPoint;
  private JobConf conf;
}

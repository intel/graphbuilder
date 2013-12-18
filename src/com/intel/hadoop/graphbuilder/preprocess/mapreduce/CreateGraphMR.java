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

import java.util.HashMap;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.io.MultiDirOutputFormat;
import com.intel.hadoop.graphbuilder.preprocess.functional.Functional;
import com.intel.hadoop.graphbuilder.preprocess.inputformat.GraphTokenizer;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.VertexEdgeUnionType;

/**
 * This MapReduce Job creates an initial edge list and vertex list from raw
 * input data, e.g. text xml. The result graph does not contain self edge and
 * duplicate vertex/edges.
 * <p>
 * The Mapper class parse each input value, provided by the {@code InputFormat},
 * and output a list of {@code Vertex} and a list of {@code Edge} using a
 * {@code GraphTokenizer}.
 * </p>
 * <p>
 * The Reducer class applies user defined {@code Functional}s to reduce
 * duplicate edges and vertices. If no such {@code Functional} is provide, it
 * outputs the first instance and discards the rest with the same identifier. It
 * also discards self edges: v - > v. An option for discard bidirectional edge
 * is provided by {@code cleanBidirectionalEdge(boolean)}.
 * </p>
 * <p>
 * Input directory: Can take multiple input directories. Output directory
 * structure:
 * <ul>
 * <li>$outputdir/edata contains edge data output</li>
 * <li>$outputdir/vdata contains vertex data output</li>
 * </ul>
 * </p>
 * 
 * @see GraphTokenizer
 */
public class CreateGraphMR {

  private static final Logger LOG = Logger.getLogger(CreateGraphMR.class);

  /**
   * Create a Job and set tokenizer and inputformat.
   * 
   * @param tokenizer
   * @param inputformat
   */
  public CreateGraphMR(GraphTokenizer tokenizer, InputFormat inputformat) {
    conf = new JobConf(CreateGraphMR.class);
    this.tokenizer = tokenizer;
    this.inputformat = inputformat;
  }

  /**
   * Set user defined function for reduce duplicate vertex and edges.
   * 
   * @param vertexfunc
   * @param edgefunc
   */
  public void setFunctionClass(Class vertexfunc, Class edgefunc) {
    try {
      if (vertexfunc != null)
        this.vertexfunc = (Functional) vertexfunc.newInstance();
      if (edgefunc != null)
        this.edgefunc = (Functional) edgefunc.newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  /**
   * Set the option to clean bidirectional edges.
   * 
   * @param clean
   *          the boolean option value, if true then clean bidirectional edges.
   */
  public void cleanBidirectionalEdge(boolean clean) {
    cleanBidirectionalEdge = clean;
  }

  /**
   * Set the intermediate key value class.
   * 
   * @param valClass
   */
  public void setValueClass(Class valClass) {
    try {
      this.mapvaltype = (VertexEdgeUnionType) valClass.newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * @return JobConf of the current job.
   */
  public JobConf getConf() {
    return conf;
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

  public void run(String[] inputpaths, String outputpath) throws Exception {
    // Mapper and Reducer
    conf.setMapperClass(CreateGraphMapper.class);
    conf.setReducerClass(CreateGraphReducer.class);

    // Key and value types
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(mapvaltype.getClass());

    // Required parameters
    conf.set("GraphTokenizer", tokenizer.getClass().getName());
    conf.setInputFormat(inputformat.getClass());
    conf.setOutputFormat(MultiDirOutputFormat.class);
    conf.setBoolean("noBidir", cleanBidirectionalEdge);

    // Optional parameters
    if (vertexfunc != null) {
      conf.set("VertexFunc", vertexfunc.getClass().getName());
    }
    if (edgefunc != null) {
      conf.set("EdgeFunc", edgefunc.getClass().getName());
    }

    for (String path : inputpaths) {
      FileInputFormat.addInputPath(conf, new Path(path));
    }
    FileOutputFormat.setOutputPath(conf, new Path(outputpath));

    LOG.info("=========== Job: Create initial graph from raw data ===========");
    LOG.info("input: " + StringUtils.join(inputpaths, ","));
    LOG.info("Output = " + outputpath);
    LOG.info("Inputformat = " + inputformat.getClass().getName());
    LOG.info("GraphTokenizer = " + tokenizer.getClass().getName());
    if (vertexfunc != null)
      LOG.info("vertexfunc = " + vertexfunc.getClass().getName());
    if (edgefunc != null)
      LOG.info("edgefunc = " + edgefunc.getClass().getName());

    if (!checkTypes()) {
      LOG.fatal("Type check failed."
          + "Please check the tokenizer is consistent with key/val types.");
      throw new Exception("Type check failed for initializing graph.");
    }
    LOG.info("==================== Start ====================================");
    JobClient.runJob(conf);
    LOG.info("=================== Done ====================================\n");
  }

  /**
   * Ensure the valuetype are consistent with the tokenizer type and functional
   * types.
   * 
   * @return true if type check.
   */
  private boolean checkTypes() {
    boolean check = true;
    if (!(tokenizer.vdataClass().equals(mapvaltype.createVdata().getClass()))) {
      LOG.fatal("VertexData Type is not consistant between MapValueType: "
          + mapvaltype.createVdata().getClass().getName() + " and Tokenizer: "
          + tokenizer.vdataClass().getName());
      check = false;
    }

    if (!(tokenizer.edataClass().equals(mapvaltype.createEdata().getClass()))) {
      LOG.fatal("EdgeDataType is not consistant between MapValueType: "
          + mapvaltype.createEdata().getClass().getName() + " and Tokenizer: "
          + tokenizer.edataClass().getName());
      check = false;
    }

    if (vertexfunc != null) {
      if (!(vertexfunc.getInType().equals(mapvaltype.createVdata().getClass()))) {
        LOG.fatal("VertexDataType is not consistant between MapValueType: "
            + mapvaltype.createEdata().getClass().getName()
            + " and the input type of VertexFunc: "
            + vertexfunc.getInType().getName());
        check = false;
      }

      if (!(vertexfunc.getOutType().equals(mapvaltype.createVdata().getClass()))) {
        LOG.fatal("VertexDataType is not consistant between MapValueType: "
            + mapvaltype.createEdata().getClass().getName()
            + " and the output type of VertexFunc: "
            + vertexfunc.getOutType().getName());
        check = false;
      }
    }

    if (edgefunc != null) {
      if (!(edgefunc.getInType().equals(mapvaltype.createEdata().getClass()))) {
        LOG.fatal("EdgeDataType is not consistant between MapValueType: "
            + mapvaltype.createEdata().getClass().getName()
            + " and the input type of EdgeFunc: "
            + edgefunc.getInType().getName());
        check = false;
      }

      if (!(edgefunc.getOutType().equals(mapvaltype.createEdata().getClass()))) {
        LOG.fatal("EdgeDataType is not consistant between MapValueType: "
            + mapvaltype.createEdata().getClass().getName()
            + " and the output type of EdgeFunc: "
            + edgefunc.getOutType().getName());
        check = false;
      }
    }
    return check;
  }

  private JobConf conf;
  private GraphTokenizer tokenizer;

  private Functional vertexfunc;
  private Functional edgefunc;

  private VertexEdgeUnionType mapvaltype;
  private InputFormat inputformat;

  private boolean cleanBidirectionalEdge;
}

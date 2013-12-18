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
package com.intel.hadoop.graphbuilder.job;

import java.util.HashMap;

import javassist.CannotCompileException;
import javassist.NotFoundException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.intel.hadoop.graphbuilder.parser.BasicGraphParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;
import com.intel.hadoop.graphbuilder.parser.ParserFactory;
import com.intel.hadoop.graphbuilder.preprocess.functional.Functional;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.EdgeTransformMR;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.EdgeTransformJobValueFactory;
import com.intel.hadoop.graphbuilder.util.FsUtil;

/**
 * An abstract wrapper class for running the Edge Transformation Job, see
 * {@code EdgeTransformMR}. An example to use this class to transform word count
 * into word frequency is illustrated in {@code EdgeTransformJobTest}.
 * <p>
 * User will need to override 2 functions for type resolving: {@code vidClass()}
 * , and {@code edataClass()}. User will also need to implement their reduce and
 * apply {@code Functiona}s, and override {@code reduceFunction()}, and
 * {@code applyFunction()}.
 * </p>
 * <p>
 * Additional options can be added into the jobConf by calling
 * {@code addUserOpt}. {@code Functional}s can get option using
 * {@code configure(JobConf)}.
 * </p>
 * <p>
 * Additional options can be added into the jobConf by calling
 * {@code addUserOpt}. {@code Functional}s can get option using
 * {@code configure(JobConf)}.
 * </p>
 * <p>
 * Input directory: list of edge data Output directory: list of transformed edge
 * data
 * </p>
 * 
 * @see EdgeTransformMR#SOURCE
 * @see EdgeTransformMR#TARGET
 * @see EdgeTransformMR
 * @see Functional
 * @see EdgeTransformJobTest
 * 
 * 
 * @param <VidType>
 * @param <VertexData>
 * @param <EdgeData>
 */
public abstract class AbstractEdgeTransformJob
<VidType extends WritableComparable<VidType>, EdgeData extends Writable, TransformedEdata extends Writable> {
  public AbstractEdgeTransformJob() {
    this.userOpts = new HashMap<String, String>();
  }

  /**
   * @return the class of vertex id type
   */
  public abstract Class vidClass();

  /**
   * @return the class of edge data type
   */
  public abstract Class edataClass();

  public abstract Functional<EdgeData, TransformedEdata> reduceFunction();

  public abstract Functional<EdgeData, TransformedEdata> applyFunction();

  /**
   * @return the class of graph parser type
   */
  public Class graphParserClass() {
    return BasicGraphParser.class;
  }

  public void addUserOpt(String key, String value) {
    userOpts.put(key, value);
  }

  /**
   * Running the transformation by grouping edges based on the
   * {@code reduceEndPoint} which is either {@code EdgeTransformMR.SOURCE} or
   * {@code EdgeTransFormMR.TARGET}.
   *
   * @param reduceEndPoint
   * @param input
   * @param output
   * @return
   * @throws NotFoundException
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws CannotCompileException
   */
  public boolean run(boolean reduceEndPoint, String input, String output)
      throws NotFoundException, InstantiationException, IllegalAccessException,
      CannotCompileException {

    GraphParser graphparser = (GraphParser) graphParserClass().newInstance();

    EdgeTransformMR mr = new EdgeTransformMR(reduceEndPoint, graphparser,
        ParserFactory.getParserByClassName(vidClass().getName()),
        ParserFactory.getParserByClassName(edataClass().getName()));

    Class valueClass =  EdgeTransformJobValueFactory
        .getValueClassByClassName(vidClass().getName(), edataClass().getName());
    mr.setKeyValueClass(vidClass(), valueClass);
    // Distribute new class file to cluster.
    FsUtil.distributedTempClassToClassPath(mr.getConf());
    mr.setFunctionClass(reduceFunction().getClass(), applyFunction().getClass());

    if (userOpts != null) {
      mr.setUserOptions(userOpts);
    }

    try {
      mr.run(input, output);
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  protected HashMap<String, String> userOpts;
}

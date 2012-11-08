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
package com.intel.hadoop.graphbuilder.test.job;

import javassist.CannotCompileException;
import javassist.NotFoundException;

import org.apache.hadoop.mapred.JobConf;

import com.intel.hadoop.graphbuilder.job.AbstractEdgeTransformJob;
import com.intel.hadoop.graphbuilder.preprocess.functional.Functional;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.EdgeTransformMR;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.PairListType;
import com.intel.hadoop.graphbuilder.types.FloatType;
import com.intel.hadoop.graphbuilder.types.IntType;
import com.intel.hadoop.graphbuilder.types.StringType;

/**
 * Test runnable for transforming the edge data of a graph. The transformation
 * function transforms the word-doc count data into word-doc frequency data.
 *
 * @author Haijie Gu
 */
public class EdgeTransformJobTest {
  public static class Sum implements Functional<IntType, FloatType> {

    @Override
    public void configure(JobConf job) throws Exception {

    }

    @Override
    public FloatType reduce(IntType a, FloatType b) {
      return new FloatType(a.get() + b.get());
    }

    @Override
    public Class<IntType> getInType() {
      return IntType.class;
    }

    @Override
    public Class<FloatType> getOutType() {
      return FloatType.class;
    }

    @Override
    public FloatType base() {
      return FloatType.ZERO;
    }
  }

  public static class Divide implements Functional<IntType, FloatType> {

    @Override
    public void configure(JobConf job) throws Exception {
    }

    @Override
    public FloatType reduce(IntType a, FloatType b) {
      return new FloatType((float) a.get() / b.get());
    }

    @Override
    public Class<IntType> getInType() {
      return IntType.class;
    }

    @Override
    public Class<FloatType> getOutType() {
      return FloatType.class;
    }

    @Override
    public FloatType base() {
      return FloatType.ONE;
    }
  }

  public static class Value extends PairListType<StringType, IntType> {

    @Override
    public StringType createLValue() {
      return new StringType();
    }

    @Override
    public IntType createRValue() {
      return new IntType();
    }
  }

  class Job extends AbstractEdgeTransformJob {
    @Override
    public Class vidClass() {
      return StringType.class;
    }

    @Override
    public Class edataClass() {
      return IntType.class;
    }

    @Override
    public Functional reduceFunction() {
      return new Sum();
    }

    @Override
    public Functional applyFunction() {
      return new Divide();
    }
  }

  /**
   * @param args
   *          [inputpath, outputpath]
   * @throws CannotCompileException
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws NotFoundException
   */
  public static void main(String[] args) throws NotFoundException,
      InstantiationException, IllegalAccessException, CannotCompileException {
    String input = args[0];
    String output = args[1];

    EdgeTransformJobTest test = new EdgeTransformJobTest();
    Job job = test.new Job();
    job.run(EdgeTransformMR.SOURCE, input, output);
  }
}

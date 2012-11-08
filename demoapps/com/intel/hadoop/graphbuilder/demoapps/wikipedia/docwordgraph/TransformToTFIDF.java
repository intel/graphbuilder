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
package com.intel.hadoop.graphbuilder.demoapps.wikipedia.docwordgraph;

import java.io.IOException;

import javassist.CannotCompileException;
import javassist.NotFoundException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.job.AbstractEdgeTransformJob;
import com.intel.hadoop.graphbuilder.preprocess.functional.Functional;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.EdgeTransformMR;
import com.intel.hadoop.graphbuilder.types.FloatType;
import com.intel.hadoop.graphbuilder.types.IntType;
import com.intel.hadoop.graphbuilder.types.StringType;

/**
 * A runnable class that transforms a word count value into tfidf value on the
 * edge.
 * @author Haijie Gu
  */
public class TransformToTFIDF {
  private static final Logger LOG = Logger.getLogger(TransformToTFIDF.class);

  /**
   * f : tf * df -> tfidf
   * @author Haijie Gu
   *
   */
  public final static class IDFfunc implements Functional<FloatType, FloatType> {
    @Override
    public FloatType reduce(FloatType a, FloatType b) {
      return new FloatType(a.get() * (float) Math.log10(numDocs / b.get()));
    }

    @Override
    public void configure(JobConf job) throws Exception {
      if (job.get("NumDocs").isEmpty()) {
        throw new Exception("NumDocs is required for IDF functional");
      }
      numDocs = job.getInt("NumDocs", 0);
    }

    @Override
    public Class<FloatType> getInType() {
      return FloatType.class;
    }

    @Override
    public Class<FloatType> getOutType() {
      return FloatType.class;
    }

    @Override
    public FloatType base() {
      return FloatType.ONE;
    }

    private int numDocs;
  }

  /**
   * f : x * y -> x + y
   * @author Haijie Gu
   *
   */
  public final static class Sumfunc implements Functional<IntType, FloatType> {
    @Override
    public void configure(JobConf job) throws Exception {
    }

    @Override
    public FloatType reduce(IntType a, FloatType b) {
      return new FloatType((float) a.get() + b.get());
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

  /**
   * f : x * y -> x / y
   */
  public final static class Dividefunc implements
      Functional<IntType, FloatType> {

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

  /**
   * f : x * y -> y + 1
   * @author Haijie Gu
   *
   */
  public final static class FloatCountFunc implements
      Functional<FloatType, FloatType> {

    @Override
    public void configure(JobConf job) throws Exception {
    }

    @Override
    public FloatType reduce(FloatType a, FloatType b) {
      return new FloatType(b.get() + 1);
    }

    @Override
    public Class<FloatType> getInType() {
      return FloatType.class;
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

  class JobTF extends AbstractEdgeTransformJob<StringType, IntType, FloatType> {

    @Override
    public Class vidClass() {
      return StringType.class;
    }

    @Override
    public Class edataClass() {
      return IntType.class;
    }

    @Override
    public Functional<IntType, FloatType> reduceFunction() {
      return new Sumfunc();
    }

    @Override
    public Functional<IntType, FloatType> applyFunction() {
      return new Dividefunc();
    }
  }

  class JobTFIDF extends
      AbstractEdgeTransformJob<StringType, FloatType, FloatType> {
    public Class vidClass() {
      return StringType.class;
    }

    public Class edataClass() {
      return FloatType.class;
    }

    public Functional<FloatType, FloatType> reduceFunction() {
      return new FloatCountFunc();
    }

    public Functional<FloatType, FloatType> applyFunction() {
      return new IDFfunc();
    }
  }

  public static void main(String[] args) throws IOException, NotFoundException,
      InstantiationException, IllegalAccessException, CannotCompileException {
    String numDocs = args[0];
    String input = args[1];
    String output = args[2];

    LOG.info(" ================ Computing TF ===================");
    JobTF job1 = new TransformToTFIDF().new JobTF();
    job1.run(EdgeTransformMR.SOURCE, input + "/edata", output + "/temp");
    JobTFIDF job2 = new TransformToTFIDF().new JobTFIDF();
    LOG.info(" ================== Compute TFIDF =======================");
    job2.addUserOpt("NumDocs", numDocs);
    job2.run(EdgeTransformMR.TARGET, output + "/temp", output + "/edata");
    LOG.info("Done");
    LOG.info("Moving vdata from " + input + " to " + output);
    try {
      FileSystem fs = FileSystem.get(new JobConf(TransformToTFIDF.class));
      fs.rename(new Path(input + "/vdata"), new Path(output + "/vdata"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
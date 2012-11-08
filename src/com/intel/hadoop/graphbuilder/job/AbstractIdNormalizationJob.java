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

import javassist.CannotCompileException;
import javassist.NotFoundException;

import com.intel.hadoop.graphbuilder.idnormalize.mapreduce.HashIdMR;
import com.intel.hadoop.graphbuilder.idnormalize.mapreduce.SortDictMR;
import com.intel.hadoop.graphbuilder.idnormalize.mapreduce.SortEdgeMR;
import com.intel.hadoop.graphbuilder.idnormalize.mapreduce.TransEdgeMR;
import com.intel.hadoop.graphbuilder.parser.BasicGraphParser;
import com.intel.hadoop.graphbuilder.parser.FieldParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;
import com.intel.hadoop.graphbuilder.parser.ParserFactory;

/**
 * An abstract wrapper class for running the Graph Id Normalization Job. User
 * needs to override 3 methods:
 * {@code vidClass(), vdataClass(), and edataClass()} to generate the right
 * parsers for parsing the input data. {@code BasicGraphParser} is used, and can
 * be replaced by overriding the {@graphParserClass()}
 * method.
 * <p>
 * See an example in {@code IdNormalizationJobTest}.
 * </p>
 *
 * Input directory:
 * <ul>
 * <li>$inputdir/edata input edge data</li>
 * <li>$inputdir/vdata input vertex data</li>
 * </ul>
 * Output directories:
 * <ul>
 * <li>$outputdir/edata edges for partition{$i}</li>
 * <li>$outputdir/vdata vertex records for partition{$i}</li>
 * <li>$outputdir/vidmap dictioanry for raw vid to normalized vid</li>
 * </ul>
 * 
 * @see BasicGraphParser
 * @see GraphParser
 * @see IdNormalizationJobTest
 * 
 * @param <VidType>
 * @param <VertexData>
 * @param <EdgeData>
 */
public abstract class AbstractIdNormalizationJob {

  /**
   * @return the class of vertex id type
   */
  public abstract Class vidClass();

  /**
   * @return the class of vertex data type
   */
  public abstract Class vdataClass();

  /**
   * @return the class of edge data type
   */
  public abstract Class edataClass();

  /**
   * @return the class of graph parser type
   */
  public Class graphParserClass() {
    return BasicGraphParser.class;
  }

  /**
   * @param n
   *          number of partitions of the dictionary
   */
  public void setDictionaryParts(int n) {
    this.numParts = n;
  }

  /**
   * Running the normalization job. Reads input from {@code input}, and outputs
   * to {@code output} directory.
   * 
   * @param nparts
   * @param inputs
   * @param output
   * @return
   * @throws NotFoundException
   * @throws CannotCompileException
   */
  public boolean run(String input, String output) throws NotFoundException,
      CannotCompileException {
    if (numParts <= 0) {
      numParts = 64;
    }

    GraphParser graphparser = null;
    try {
      graphparser = (GraphParser) graphParserClass().newInstance();
    } catch (InstantiationException e1) {
      e1.printStackTrace();
    } catch (IllegalAccessException e1) {
      e1.printStackTrace();
    }
    FieldParser vidparser = ParserFactory.getParserByClassName(vidClass()
        .getName());
    FieldParser vdataparser = ParserFactory.getParserByClassName(vdataClass()
        .getName());
    FieldParser edataparser = ParserFactory.getParserByClassName(edataClass()
        .getName());

    try {
      HashIdMR job1 = new HashIdMR(graphparser, vidparser, vdataparser);
      job1.run(input + "/vdata", output);

      SortDictMR job2 = new SortDictMR(numParts, true, vidparser);
      job2.run(output + "/vidmap", output + "/temp/partitionedvidmap");

      SortEdgeMR job3 = new SortEdgeMR(numParts, graphparser, vidparser,
          edataparser);
      job3.run(input + "/edata", output + "/temp/partitionededata");

      TransEdgeMR job4 = new TransEdgeMR(numParts, output
          + "/temp/partitionedvidmap", graphparser, vidparser, edataparser);
      job4.run(output + "/temp/partitionededata", output + "/edata");
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  private int numParts;
}

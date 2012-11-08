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

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.intel.hadoop.graphbuilder.parser.BasicGraphParser;
import com.intel.hadoop.graphbuilder.parser.GraphParser;
import com.intel.hadoop.graphbuilder.parser.ParserFactory;
import com.intel.hadoop.graphbuilder.partition.mapreduce.edge.EdgeIngressMR;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.IngressJobKeyValueFactory;
import com.intel.hadoop.graphbuilder.partition.mapreduce.vrecord.VrecordIngressMR;
import com.intel.hadoop.graphbuilder.util.FsUtil;

/**
 * An abstract wrapper class for running the Partitioning Job. User needs to
 * override 3 methods: {@code vidClass(), vdataClass(), and edataClass()} to
 * generate the right parsers for parsing the input data.
 * {@code BasicGraphParser} is used, and can be replaced by overriding the
 * {@graphParserClass()} method.
 * <p>
 * See an example in {@code PartitionJobTest}.
 * </p>
 *
 * Input directories contains hybrid of edge and vertex data. Output
 * directories:
 * <ul>
 * <li>$outputdir/edges/partition{$i} edges for partition{$i}</li>
 * <li>$outputdir/vrecords/partition{$i} vertex records for partition{$i}</li>
 * </ul>
 *
 * @see BasicGraphParser
 * @see GraphParser
 * @see PartitionJobTest
 *
 * @param <VidType>
 * @param <VertexData>
 * @param <EdgeData>
 */
public abstract class AbstractPartitionJob<VidType extends WritableComparable<VidType>, VertexData extends Writable, EdgeData extends Writable> {

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
   * Running the partitioning job with {@code nparts} partitions. Reads input
   * from {@code inputs}, and outputs to {@code output} directory.
   *
   * @param nparts
   * @param inputs
   * @param output
   * @return
   * @throws NotFoundException
   * @throws CannotCompileException
   */
  public boolean run(int nparts, String[] inputs, String output)
      throws NotFoundException, CannotCompileException {
    EdgeIngressMR job1 = new EdgeIngressMR(
        graphParserClass(),
        ParserFactory
        .getParserByClassName(vidClass().getName()).getClass(),
        ParserFactory
        .getParserByClassName(vdataClass().getName()).getClass(),
        ParserFactory
        .getParserByClassName(edataClass().getName()).getClass());
    job1.setKeyValueClass(IngressJobKeyValueFactory
        .getKeyClassByClassName(vidClass().getName()),
        IngressJobKeyValueFactory.getValueClassByClassName(
            vidClass().getName(), vdataClass().getName(), edataClass()
                .getName()));
    // Distribute new class file to cluster.
    FsUtil.distributedTempClassToClassPath(job1.getConf());
    VrecordIngressMR job2 = new VrecordIngressMR();

    try {
      job1.run(inputs, output + "/edges", nparts, "greedy");
      job2.run(nparts, output + "/edges/vrecord", output + "/vrecords");
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }
}

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
package com.intel.hadoop.graphbuilder.test.partition;

import com.intel.hadoop.graphbuilder.parser.BasicGraphParser;
import com.intel.hadoop.graphbuilder.parser.EmptyParser;
import com.intel.hadoop.graphbuilder.parser.IntParser;
import com.intel.hadoop.graphbuilder.partition.mapreduce.edge.EdgeIngressMR;
import com.intel.hadoop.graphbuilder.partition.mapreduce.keyvalue.IngressJobKeyValueFactory;
import com.intel.hadoop.graphbuilder.types.TypeFactory;

/**
 * A runnable EdgeIngress Mapreduce taking arguments from commandline. Example:
 * hadoop jar target/graphbuilder-0.0.1-SNAPSHOT-hadoop-job.jar
 * com.intel.hadoop.graphbuilder.test.partition.EdgeIngressTest input output 8
 * oblivious
 * 
 * @author Haijie Gu
 */
public class EdgeIngressTest {
  public static void main(String[] args) throws Exception {
    EdgeIngressMR mr = new EdgeIngressMR(BasicGraphParser.class,
        IntParser.class, EmptyParser.class, EmptyParser.class);
    mr.useGzip(true);
    // mr.setKeyValueClass(IntIngressKeyType.class,
    // Empty2IngressValueType.class);
    Class keyClass = IngressJobKeyValueFactory
        .getKeyClassByClassName(TypeFactory.getClassName("int"));
    Class valClass = IngressJobKeyValueFactory.getValueClassByClassName(
        TypeFactory.getClassName("int"), TypeFactory.getClassName("none"),
        TypeFactory.getClassName("none"));
    mr.setKeyValueClass(keyClass, valClass);
    mr.run(new String[] { args[0] }, args[1], Integer.valueOf(args[2]), args[3]);
  }
}
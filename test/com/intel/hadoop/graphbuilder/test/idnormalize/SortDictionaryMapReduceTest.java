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
package com.intel.hadoop.graphbuilder.test.idnormalize;

import java.io.IOException;

import com.intel.hadoop.graphbuilder.idnormalize.mapreduce.SortDictMR;
import com.intel.hadoop.graphbuilder.parser.StringParser;

/**
 * A runnable SortDictionary Mapreduce taking arguments from commandline.
 * Example: hadoop jar target/graphbuilder-0.0.1-SNAPSHOT-hadoop-job.jar
 * com.intel.hadoop.graphbuilder.test.idnormalize.SortDictionaryMapReduceTest
 * input output
 * 
 * @author Haijie Gu
 */
public class SortDictionaryMapReduceTest {
  public static void main(String[] args) throws IOException {
    SortDictMR driver = new SortDictMR(32, true, new StringParser());
    driver.run(args[0], args[1]);
  }
}

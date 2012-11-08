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
package com.intel.hadoop.graphbuilder.demoapps.wikipedia;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.intel.hadoop.graphbuilder.preprocess.inputformat.XMLInputFormat;
import com.intel.hadoop.graphbuilder.preprocess.inputformat.XMLInputFormat.XMLRecordReader;

public class WikiPageInputFormat extends FileInputFormat<LongWritable, Text> {

  public static final String START_TAG = "<page>";
  public static final String END_TAG = "</page>";

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
      JobConf conf, Reporter reporter) throws IOException {
    conf.set(XMLInputFormat.START_TAG_KEY, START_TAG);
    conf.set(XMLInputFormat.END_TAG_KEY, END_TAG);
    return new XMLRecordReader((FileSplit) split, conf);
  }
}

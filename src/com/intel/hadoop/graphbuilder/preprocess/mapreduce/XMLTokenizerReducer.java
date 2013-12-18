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
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.intel.hadoop.graphbuilder.preprocess.functional.Functional;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.VertexEdgeUnionType;
import com.intel.hadoop.graphbuilder.util.Pair;

/**
 * The Reducer class applies user defined {@code Functional}s to reduce
 * duplicate edges and vertices. If no such {@code Functional} is provide, it
 * outputs the first instance and discards the rest with the same identifier. It
 * also discards self edges: v - > v. An option for discard bidirectional edge
 * is provided by {@code cleanBidirectionalEdge(boolean)}.
 * <p>
 * Output directory structure:
 * <ul>
 * <li>$outputdir/edata contains edge data output</li>
 * <li>$outputdir/vdata contains vertex data output</li>
 * </ul>
 * </p>
 * 
 */
public class XMLTokenizerReducer extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {

	@Override
		public void configure(JobConf job) {
			super.configure(job);
			this.valClass = job.getMapOutputValueClass();
			this.noBidir = job.getBoolean("noBidir", false);
		}

	@Override
		public void reduce(Text key, Iterator<Text> iter,
				OutputCollector<Text, Text> out, Reporter reporter) throws IOException {

			Text next;
			while (iter.hasNext()) {
				next = iter.next();
				//out.collect(new Text("xmldata"), new Text(key + "\t" + next));
				out.collect(new Text("xmldata"), new Text(key + "#::#" + next));
			}
		}

	protected boolean noBidir;
	protected Class keyClass;
	protected Class valClass;
}

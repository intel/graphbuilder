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
package com.intel.hadoop.graphbuilder.io;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

/**
 * OutputFormat which allows to multiplex output directory based on the output
 * key. If key does not contains space, then it is treated as the output
 * directory. If key does contains space, then its first part is the directory,
 * and the second part is the file name.
 * 
 */
public class MultiDirOutputFormat extends MultipleTextOutputFormat<Text, Text> {

  @Override
  protected Text generateActualKey(Text key, Text value) {
    return null;
  }

  @Override
  protected String generateFileNameForKeyValue(Text key, Text value, String name) {
    String[] path = key.toString().split(" ");
    if (path.length > 1) {
      return new Path(path[0], path[1]).toString();
    } else {
      return new Path(key.toString(), name).toString();
    }
  }

}

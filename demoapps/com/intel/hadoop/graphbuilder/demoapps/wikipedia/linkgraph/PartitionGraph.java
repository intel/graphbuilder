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
package com.intel.hadoop.graphbuilder.demoapps.wikipedia.linkgraph;

import java.io.IOException;

import javassist.CannotCompileException;
import javassist.NotFoundException;

import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.job.AbstractPartitionJob;
import com.intel.hadoop.graphbuilder.types.EmptyType;
import com.intel.hadoop.graphbuilder.types.StringType;

public class PartitionGraph {

  private static final Logger LOG = Logger.getLogger(PartitionGraph.class);

  class Job extends AbstractPartitionJob {

    @Override
    public Class vidClass() {
      return StringType.class;
    }

    @Override
    public Class vdataClass() {
      return EmptyType.class;
    }

    @Override
    public Class edataClass() {
      return EmptyType.class;
    }

  }

  public static void main(String[] args) throws IOException,
      InstantiationException, IllegalAccessException, NotFoundException,
      CannotCompileException {
    int npart = Integer.parseInt(args[0]);
    String input = args[1];
    String output = args[2];
	String ingress = args[3];
    LOG.info("========== Partitioning Graph ============");
    new PartitionGraph().new Job().run(npart, new String[] { input + "/vdata",
        input + "/edata" }, output, Integer.parseInt(ingress));
    LOG.info("========== Done partitioning graph ============");
  }
}

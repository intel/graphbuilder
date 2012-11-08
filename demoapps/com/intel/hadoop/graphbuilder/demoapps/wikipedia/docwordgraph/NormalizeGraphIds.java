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

import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.job.AbstractIdNormalizationJob;
import com.intel.hadoop.graphbuilder.types.EmptyType;
import com.intel.hadoop.graphbuilder.types.FloatType;
import com.intel.hadoop.graphbuilder.types.StringType;

public class NormalizeGraphIds {
  private static final Logger LOG = Logger.getLogger(NormalizeGraphIds.class);

  class Job extends AbstractIdNormalizationJob {

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
      return FloatType.class;
    }
  }

  public static void main(String[] args) throws IOException,
      InstantiationException, IllegalAccessException, NotFoundException,
      CannotCompileException {
    String input = args[0];
    String output = args[1];

    LOG.info("========== Normalizing Graph ============");
    new NormalizeGraphIds().new Job().run(input, output);
    LOG.info("========== Done normalizing graph ============");
  }
}

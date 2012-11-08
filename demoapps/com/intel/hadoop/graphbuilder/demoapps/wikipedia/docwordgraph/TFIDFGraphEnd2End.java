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

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.util.Timer;

/**
 * An end 2 end job flow for creating an Document-Word bipartie graph,
 * with TFIDF on the edge from a wikipedia xml dump.
 * @author Haijie Gu
 */
public class TFIDFGraphEnd2End {
  private static final Logger LOG = Logger.getLogger(TFIDFGraphEnd2End.class);

  /**
   * @param args
   * @throws IOException
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws ParserConfigurationException
   * @throws NotFoundException
   * @throws CannotCompileException
   */
  public static void main(String[] args) throws IOException,
      InstantiationException, IllegalAccessException, CannotCompileException,
      NotFoundException, ParserConfigurationException {

    int npart = Integer.parseInt(args[0]);
    String rawinput = args[1];
    String output = args[2];

    String rawgraph = output + "/graph_raw";
    String tfidfgraph = output + "/graph_tfidf";
    String normedgraph = output + "/graph_norm";
    String partitionedgraph = output + "/graph_partitioned";

    Timer timer = new Timer();
    timer.start();


    try {
    if (args.length > 3) {
      String dict = args[3];
      new CreateWordCountGraph()
          .main(new String[] { rawinput, rawgraph, dict });
    } else {
      new CreateWordCountGraph().main(new String[] { rawinput, rawgraph });
    }

    LOG.info("Create graph finished in : " + timer.time_since_last()
        + " seconds");


    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    try {
      new TransformToTFIDF().main(new String[] { String.valueOf(30000), rawgraph,
          tfidfgraph });
      LOG.info("Transform TFIDF finished in : " + timer.time_since_last()
        + " seconds");
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    try {
    new NormalizeGraphIds().main(new String[] { tfidfgraph, normedgraph });

    LOG.info("Normalize graph finished in : " + timer.time_since_last()
        + " seconds");
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    try {
      new PartitionGraph().main(new String[] { String.valueOf(npart),
        normedgraph, partitionedgraph });

      LOG.info("Partition graph finished in : " + timer.time_since_last()
        + " seconds");
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    LOG.info("Total flow time : " + timer.current_time() + " seconds");
  }

}

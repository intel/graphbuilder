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

import com.intel.hadoop.graphbuilder.partition.mapreduce.edge.EdgeIngressMR;

/**
 * An end 2 end job flow for creating a Word-Page bipartie graph
 * with TFIDF on the edge from a wikipedia xml dump.
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

	if (args.length < 3) {
		System.out.println("Usage:\tTFIDFGraphEnd2End <# of partitions> <input-dir> <output-dir> <edge ingress method> [<enable-tfidf>]");
		return;
	}

    int npart = Integer.parseInt(args[0]);
    String rawinput = args[1];
    String output = args[2];
	String ingressCode = args[3];
    int enable_tfidf = 0;
    int dict_stopword_type = 0; // 0: no dictionary, no stop words list; 1: only dictionary; 2: only stop words list; 3: both dictionary and stop words list.

    String rawgraph = output + "/graph_raw";
    String tfidfgraph = output + "/graph_tfidf";
    String normedgraph = output + "/graph_norm";
    String partitionedgraph = output + "/graph_partitioned";
    String input2norm = rawgraph;

    String dict = "";
    String stopwords ="";
		
    Timer timer = new Timer();
    timer.start();

    Integer ic = Integer.parseInt(ingressCode);
    if (ic > EdgeIngressMR.IngressCodeLimit) {
		System.out.println("ERROR: IngressCode " + ic + " is invalid. It must be 0-" + EdgeIngressMR.IngressCodeLimit);
        return;
    }

    if(args.length > 4) enable_tfidf = Integer.parseInt(args[4]);
    
    if(enable_tfidf==1)
        System.out.println("Enable calculating TFIDF..");

    if (args.length > 5 ) {
         dict_stopword_type = Integer.parseInt(args[5]);
        switch (dict_stopword_type) {
                case 0:  
                             break;
                case 1:  
				if (args.length > 6)  dict = args[6];
    			       else System.out.println("Please input dictionary"); 
                             break;
                case 2: 
			       if (args.length > 6)  stopwords = args[6];
    			       else System.out.println("Please input stop words list."); 
                             break;
                case 3:  
				if (args.length > 7) {
                                 dict = args[6];
                                 stopwords = args[7];
    			        } 
    			      else System.out.println("Please input both dictionary and stop words list."); 
                            break;
    	    default: System.out.println("Invalid type for dictionary and stop words."); 
    			   break;
        	}

	} 

    try {
      if (dict_stopword_type >1) {
        new CreateWordCountGraph()
          .main(new String[] { rawinput, rawgraph, dict, stopwords});
      } else if (dict_stopword_type == 1) {
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
    
    if (enable_tfidf==1) {
        try {
            new TransformToTFIDF().main(new String[] { String.valueOf(30000), rawgraph,
                    tfidfgraph });
            LOG.info("Transform TFIDF finished in : " + timer.time_since_last()
                    + " seconds");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        input2norm = tfidfgraph;
    }

    try {
      new NormalizeGraphIds().main(new String[] { input2norm, normedgraph });
      LOG.info("Normalize graph finished in : " + timer.time_since_last()
        + " seconds");
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    try {
      new PartitionGraph().main(new String[] { String.valueOf(npart),
        normedgraph, partitionedgraph, ingressCode });

      LOG.info("Partition graph finished in : " + timer.time_since_last()
        + " seconds");
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
   
    LOG.info("Total flow time : " + timer.current_time() + " seconds");
  }
}

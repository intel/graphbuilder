/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.pipeline;

import com.intel.hadoop.graphbuilder.graphelements.ValueClassFactory;
import com.intel.hadoop.graphbuilder.pipeline.output.OutputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration;
import com.intel.hadoop.graphbuilder.pipeline.output.GraphGenerationMRJob;
import com.intel.hadoop.graphbuilder.pipeline.tokenizer.GraphBuildingRule;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.commons.cli.CommandLine;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.io.IOException;

/**
 * The class that is responsible for resolving and configuring the graph 
 * construction pipeline prior to execution.
 *
 * <p>
 *  Stages include:
 *  <ul>
 *      <li>The input configuration (raw input into records).</li>
 *      <li>The graphbuilding rule (records into property graph elements).</li>
 *      <li>Duplicate removal settings (are duplicates simply merged, do we 
 *          keep bidirectional edges, and so on).</li>
 *      <li>Graph storage (to an output file on HDFS in a chosen format, or 
 *          perhaps to a graph database).</li>
 *  </ul>
 * </p>
 *
 * @see InputConfiguration
 * @see GraphBuildingRule
 * @see OutputConfiguration
 *
 */

public class GraphConstructionPipeline {

    private static final Logger LOG = Logger.getLogger(GraphConstructionPipeline.class);

    private HashMap<String, String> userOpts;

    /**
     * Checks if the remove duplicates phase treats bidirectional edges as 
	 * duplicates and removes them, or not.
     */
    public enum BiDirectionalHandling {
        KEEP_BIDIRECTIONALEDGES,
        REMOVE_BIDIRECTIONALEDGES
    }

    /**
     * The constructor.
     */
    public GraphConstructionPipeline() {
        this.userOpts = new HashMap<String, String>();
    }

    /**
     * Adds a user option to the configuration that will be available at run   
     * time in the subsequent Hadoop jobs of the graph construction pipeline.
     * @param {@code key}    The key of the user option.
     * @param {@code value}  The value of the user option.
     */
    public void addUserOpt(String key, String value) {
        userOpts.put(key, value);
    }

    /**
     * Takes the pipeline settings and runs a graph construction process.
     *
     * @param {@code inputConfiguration}
     * @param {@code graphBuildingRule}
     * @param {@code cleanBiDirectionalEdges}
     * @param {@code outputConfiguration}
     * @param {@code cmd}
     */
    public void run(InputConfiguration    inputConfiguration,
                    GraphBuildingRule     graphBuildingRule,
                    BiDirectionalHandling cleanBiDirectionalEdges,
                    OutputConfiguration   outputConfiguration,
                    CommandLine           cmd) {


        GraphGenerationMRJob graphGenerationMRJob = outputConfiguration.getGraphGenerationMRJob();

        // "Hook up" the input configuration and tokenizer to the MR Job specified by the output configuration.

        graphGenerationMRJob.init(inputConfiguration, graphBuildingRule);

        Class vidClass   = graphBuildingRule.vidClass();
        Class valueClass = ValueClassFactory.getValueClassByVidClassName(vidClass.getName());

        graphGenerationMRJob.setVidClass(vidClass);
        graphGenerationMRJob.setValueClass(valueClass);

        // Sets the optional parameters.

        graphGenerationMRJob.setCleanBidirectionalEdges(cleanBiDirectionalEdges == BiDirectionalHandling.KEEP_BIDIRECTIONALEDGES);

        // Sets the user defined parameters.

        if (userOpts != null) {
            graphGenerationMRJob.setUserOptions(userOpts);
        }

        try {
            graphGenerationMRJob.run(cmd);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "GRAPHBUILDER_ERROR: IO Exception during map-reduce job execution.", LOG, e);
        }  catch (ClassNotFoundException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.CLASS_INSTANTIATION_ERROR,
                    "GRAPHBUILDER_ERROR: Class not found exception during map-reduce job execution.", LOG, e);
        }  catch (InterruptedException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.HADOOP_REPORTED_ERROR,
                    "GRAPHBUILDER_ERROR: Interruption during map-reduce job execution.", LOG, e);
        }
    }
}

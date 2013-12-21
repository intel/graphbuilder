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
package com.intel.hadoop.graphbuilder.pipeline.input.hbase;

import com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElement;
import com.intel.hadoop.graphbuilder.util.GraphBuilderExit;
import com.intel.hadoop.graphbuilder.util.HBaseUtils;
import com.intel.hadoop.graphbuilder.util.StatusCode;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * This class handles the set-up time configuration when the raw input is an Hbase table.
 *
 * For graph construction tasks that require multiple chained Map Reduce jobs, this class 
 * affects only the first Map Reduce job, as that is the first mapper that deals with raw input.
 *
 * <ul>
 * <li> It provides a handle to the mapper class used to read hbase tables ({@code HBaseReaderMapper}).</li>
 * <li> It prepares the MR job and configuration by calling hbase utilities.</li>
 * </ul>
 *
 * The Constructor will terminate the process if it cannot connect to HBase.
 *
 * @see com.intel.hadoop.graphbuilder.pipeline.input.InputConfiguration
 * @see HBaseReaderMapper
 *
 */

public class HBaseInputConfiguration implements InputConfiguration {

    private static final Logger LOG = Logger.getLogger(HBaseInputConfiguration.class);

    private HBaseUtils hBaseUtils   = null;
    private String     srcTableName = null;

    private Scan       scan         = new Scan();

    private Class      mapperClass  = HBaseReaderMapper.class;

    /**
     * Allocates and acquires an instance of the singleton {@code HBaseUtils}.
     */
    public HBaseInputConfiguration(String srcTableName) {

        this.srcTableName = srcTableName;

        try {
            this.hBaseUtils = HBaseUtils.getInstance();
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNABLE_TO_CONNECT_TO_HBASE,
                    "GRAPHBUILDER_ERROR: Cannot allocate the HBaseUtils object. Check hbase connection.", LOG, e);
        }

        try {
            if (!hBaseUtils.tableExists(srcTableName)) {
                GraphBuilderExit.graphbuilderFatalExitNoException(StatusCode.MISSING_HBASE_TABLE,
                        "GRAPHBUILDER ERROR: " + srcTableName + " table does not exist", LOG);
            }
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.UNHANDLED_IO_EXCEPTION,
                    "GRAPHBUILDER ERROR: IO exception when attempting to read HBase table " + srcTableName, LOG, e);
        }
    }

    /**
     * This input configuration uses hbase.
     * @return  {@literal true }
     */
    public boolean usesHBase() {
        return true;
    }

    /**
     * Performs setup tasks with hbase.
     * @param {@code configuration} The configuration being prepared for the graph construction job.
     */

    public void updateConfigurationForMapper(Configuration configuration) {

        configuration.set("SRCTABLENAME", srcTableName);


        scan.setCaching(HBaseConfig.config.getPropertyInt("HBASE_CACHE_SIZE"));
        scan.setCacheBlocks(false);
    }

    /**
     * Initializes the table mapper job.
     * @param {@code job}  The map reduce job in preparation for graph construction.
     */
    public void updateJobForMapper(Job job) {
        try {
            TableMapReduceUtil.initTableMapperJob(srcTableName, scan, HBaseReaderMapper.class, Text.class, SerializedGraphElement.class, job);
        } catch (IOException e) {
            GraphBuilderExit.graphbuilderFatalExitException(StatusCode.HADOOP_REPORTED_ERROR,
                    "GRAPHBUILDER_ERROR: Could not initialize table mapper job", LOG, e);
        }
    }

    /**
     * Returns the class of the mapper used.
     * @return {@code HBaseReaderMapper.class}
     * @see HBaseReaderMapper
     */
    public Class getMapperClass() {
        return mapperClass;
    }

    /**
     * Obtains a description of the input configuration for logging purposes.
     * @return  "Hbase table name: " Appended with the source table name.
     */
    public String getDescription() {
        return "Hbase table name: " + srcTableName;
    }
}

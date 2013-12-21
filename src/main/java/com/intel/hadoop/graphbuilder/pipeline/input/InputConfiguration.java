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
package com.intel.hadoop.graphbuilder.pipeline.input;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * This interface encapsulates the methods used to configure an end-to-end map reduce chain so
 * that it will work with the input format specified.
 *
 * The first map job corresponds to stepping through the raw input and spitting out property 
 * graph elements. The input determines the first mapper.
 *
 * The output determines the first reducer and any subsequent Map Reducer tasks in a chain.
 *
 * The methods in this interface are used by the full Map Reducer chain to properly configure the first 
 * Map Reducer job to work with the input mapper.
 *
 */

public interface InputConfiguration {

    public void    updateConfigurationForMapper (Configuration configuration);

    public void    updateJobForMapper(Job job);

    public boolean usesHBase();

    public Class   getMapperClass();

    public String  getDescription();
}

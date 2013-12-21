/* Copyright (C) 2013 Intel Corporation.
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

/**
* This script should be run from the top level directory
* Demonstrates how to bulk load the Titan graph database
*/

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;
IMPORT 'pig/graphbuilder.pig';

--prepare temp storage that is used by the LOAD_TITAN macro
--the temp storage is required for doing a dummy LOAD/STORE for the 
--MAPREDUCE operator use in that macro
rmf /tmp/empty
fs -mkdir /tmp/empty
rmf /tmp/tmp_store_1;
rmf /tmp/tmp_store_2;

employees = LOAD 'examples/data/employees.csv' USING PigStorage(',') AS 
		(id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, underManager:chararray);
employees_with_valid_ids = FILTER employees BY id!='';

--GB requires the input data to be in HBase so
--we need to append HBase row keys to the input relation 
final_relation = FOREACH employees_with_valid_ids GENERATE FLATTEN(CreateRowKey(*));

--create GB input table
sh echo "disable 'gb_input_table'" | hbase shell
sh echo "drop 'gb_input_table'" | hbase shell
sh echo "create 'gb_input_table', {NAME=>'cf'}" | hbase shell --cf is the column family

STORE final_relation INTO 'hbase://gb_input_table' 
  		USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf:id cf:name cf:age cf:dept cf:manager cf:underManager');
	  
--build an undirected graph with the --edges argument		
-- -O flag specifies overwriting the input Titan table
LOAD_TITAN('gb_input_table', '"cf:id=cf:name,cf:age,cf:dept" "cf:manager"',
			   '--edges "cf:id,cf:manager,worksUnder,cf:underManager"',
			   'examples/hbase-titan-conf.xml', '-O'); 
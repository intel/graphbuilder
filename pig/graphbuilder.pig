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

DEFINE ExtractJSONField com.intel.pig.udf.eval.ExtractJSONField();
DEFINE CreateRowKey com.intel.pig.udf.eval.CreateRowKey();
DEFINE RegexExtractAllMatches com.intel.pig.udf.eval.RegexExtractAllMatches();
DEFINE FlattenAsGBString com.intel.pig.udf.eval.FlattenAsGBString();

/**
 * Remove duplicates from a given relation of property graph elements.
 *
 * <p><ul>
 * <li>Vertices with the same IDs are treated as equal</li>
 * <li>Edges with the same label and matching vertex IDs on their source and destination vertices are treated as equal</li>
 * <li>Property lists are merged between duplicates, with conflicts resolved arbitrarily. </li>
 * </ul> </p>
 * @param inPropGraph relation of property graph elements to remove the duplicates from
 *
*/

DEFINE MERGE_DUPLICATE_ELEMENTS(inPropGraph) RETURNS outPropGraph {
  DEFINE GetPropGraphEltID com.intel.pig.udf.eval.GetPropGraphElementID;
  labeled = FOREACH $inPropGraph GENERATE (GetPropGraphEltID(*)), $0;
  grouped = GROUP labeled BY $0;
  DEFINE Merge com.intel.pig.udf.eval.MergeDuplicateGraphElements;
  $outPropGraph = FOREACH grouped GENERATE FLATTEN(Merge(*));
};

/**
* LOAD_TITAN macro bulk loads a graph into the Titan graph database and also passes the given
* arguments to GB for further configuration. This macro
* is a wrapper for the com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB MapReduce job.
* For the details of the command line arguments @see com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB.
* 
* @param input_hbase_table_name : name of the input HBase table that GraphBuilder (GB) will create the graph from <br/>
* @param  vertex_rule 		    : vertex creation rule \link com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule see HBaseGraphBuildingRule \endlink <br/>
* @param  edge_rule			    : edge creation rule \link com.intel.hadoop.graphbuilder.pipeline.tokenizer.hbase.HBaseGraphBuildingRule see HBaseGraphBuildingRule \endlink <br/>
* @param  config_file		    : path to the XML configuration file to be used by GB for bulk loading to Titan <br/>
* @param other_args				: other command line arguments to \link com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB TableToGraphDB \endlink <br/>
*/
DEFINE LOAD_TITAN(input_hbase_table_name, vertex_rule, edge_rule, config_file, other_args) RETURNS void {
	-- load an empty relation that will be used in the MAPREDUCE operator
	dummy = LOAD '/tmp/empty' USING TextLoader() AS (line:chararray);
	
	-- we do a dummy STORE/LOAD below as the MAPREDUCE operator 
	-- requires a STORE and then a LOAD operation
	
	stored_graph = MAPREDUCE 'target/graphbuilder-2.0-alpha-with-deps.jar' 
	  		STORE dummy INTO '/tmp/tmp_store_1'
	  		LOAD '/tmp/empty' USING TextLoader() AS (line:chararray) 
	  		`com.intel.hadoop.graphbuilder.sampleapplications.TableToGraphDB -conf $config_file --tablename $input_hbase_table_name --vertices $vertex_rule $edge_rule $other_args`;
	  		
	STORE stored_graph INTO '/tmp/tmp_store_2';
};

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
* Demonstrates how to generate RDF triples from property graph elements
*/

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;
IMPORT 'pig/graphbuilder.pig';

rmf /tmp/rdf_triples; --delete the output directory

-- Customize the way property graph elements are created from raw input
DEFINE CreatePropGraphElements com.intel.pig.udf.eval.CreatePropGraphElements('-v "OWL.People,id=name,age,dept" "OWL.People,manager" -e "id,manager,OWL.worksUnder,underManager"');

--specify the RDF namespace to use
DEFINE RDF com.intel.pig.udf.eval.RDF('OWL');

employees = LOAD 'examples/data/employees.csv' USING PigStorage(',') 
				AS (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, underManager:chararray);
employees_with_valid_ids = FILTER employees BY id!='';
pge = FOREACH employees_with_valid_ids GENERATE FLATTEN(CreatePropGraphElements(*)); -- generate the property graph elements
merged = MERGE_DUPLICATE_ELEMENTS(pge);
rdf_triples = FOREACH merged GENERATE FLATTEN(RDF(*)); -- generate the RDF triples
DESCRIBE rdf_triples;
STORE rdf_triples INTO '/tmp/rdf_triples' USING PigStorage();

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

-- Delete the output directory
rmf /tmp/rdf_triples; 

-- Customize the way property graph elements are created from raw input
DEFINE CreatePropertyGraph com.intel.pig.udf.eval.CreatePropGraphElements;

--specify the RDF namespace to use
DEFINE RDF com.intel.pig.udf.eval.RDF;

-- Load in the example data
employees = LOAD 'examples/data/employees.csv' USING PigStorage(',') 
            AS (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, serviceLength:chararray);
employees_with_valid_ids = FILTER employees BY id!='';

-- Generate the property graph
-- Firstly transform the employee tuples to add the property graph mapping to each tuple
employeesWithMappings = FOREACH employees_with_valid_ids GENERATE (*, [ 'vertices' # ( [ 'id' # 'id', 'properties' # ('name', 'age', 'dept', 'serviceLength'), 'labels' # [ 'type' # 'Person' ] ], 
                                                                                       [ 'id' # 'manager', 'labels' # [ 'type' # 'Manager' ] ] ),
                                                                        'edges' # ( [ 'source' # 'id', 'target' # 'manager', 'label' # 'hasManager', 'inverseLabel' # 'manages' ] ) ]
                                                                  );

propertyGraph = FOREACH employeesWithMappings GENERATE FLATTEN(CreatePropertyGraph(*));
-- DUMP propertyGraph;

-- Generate the RDF triples
propertyGraphWithMappings = FOREACH propertyGraph GENERATE (*, [ 'idBase' # 'http://example.org/instances/', 'base' # 'http://example.org/ontology/',
                                                                 'namespaces' # [ 'foaf' # 'http://xmlns.com/foaf/0.1' ],
                                                                 'propertyMap' # [ 'type' # 'a', 'name' # 'foaf:name', 'age' # 'foaf:age' ],
                                                                 'idProperty' # 'id' ]);
rdf_triples = FOREACH propertyGraphWithMappings GENERATE FLATTEN(RDF(*)); -- generate the RDF triples
--DESCRIBE rdf_triples;
--STORE rdf_triples INTO '/tmp/rdf_triples' USING PigStorage();
DUMP rdf_triples;
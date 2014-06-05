/* Copyright (C) 2013 Intel Corporation.
 * Copyright 2014 YarcData LLC
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
* Demonstrates how to generate a property graph and then transform that
* into RDF
*/

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar;
IMPORT 'pig/graphbuilder.pig';

-- Delete the output directory
rmf /tmp/rdf_triples; 

-- Define our UDFs we'll use later
DEFINE CreatePropertyGraph com.intel.pig.udf.eval.CreatePropGraphElements;
DEFINE RDF com.intel.pig.udf.eval.RDF;

-- Load in the example data and filter out any entries with an invalid ID
employees = LOAD 'examples/data/employees.csv' USING PigStorage(',') 
            AS (id:chararray, name:chararray, age:chararray, dept:chararray, manager:chararray, serviceLength:chararray);
employees_with_valid_ids = FILTER employees BY id!='';

-- Generate the property graph

-- Firstly transform the employee tuples to add the property graph mapping to each tuple
-- In this example we provide two mappings that produce vertices and a single mapping to produce edges
-- See the javadoc for the PropertyGraphMapping class to understand the format of the mapping
employeesWithMappings = FOREACH employees_with_valid_ids GENERATE (*, [ 'vertices' # ( [ 'id' # 'id', 'properties' # ('name', 'age', 'dept', 'serviceLength'), 'labels' # [ 'type' # 'Person' ] ], 
                                                                                       [ 'id' # 'manager', 'labels' # [ 'type' # 'Manager' ] ] ),
                                                                        'edges' # ( [ 'source' # 'id', 'target' # 'manager', 'label' # 'hasManager', 'inverseLabel' # 'manages' ] ) ]
                                                                  );

-- Then we actually apply the mapping, the use of FLATTEN is required since each tuple produces a bag
-- of property graph elements and we need them as individual tuples to work on them later
propertyGraph = FOREACH employeesWithMappings GENERATE FLATTEN(CreatePropertyGraph(*));

-- Generate the RDF triples

-- Firstly transform the property graph elements tuples to add the RDF mapping to each tuple
-- Here we have a mapping with separate Base URIs for properties and vertices, it also maps
-- specific properties in the property graph to specific RDF URIs using namespaces to provide
-- prefixed name based representation of these
-- See the javadoc for the RdfMapping class to understand the format of the mapping
propertyGraphWithMappings = FOREACH propertyGraph GENERATE (*, [ 'idBase' # 'http://example.org/instances/', 'base' # 'http://example.org/ontology/',
                                                                 'namespaces' # [ 'foaf' # 'http://xmlns.com/foaf/0.1/' ],
                                                                 'propertyMap' # [ 'type' # 'a', 'name' # 'foaf:name', 'age' # 'foaf:age' ],
                                                                 'idProperty' # 'id' ]);
                                                                 
-- Then we generate the actual triples, again we need to use FLATTEN since each input tuple produces a
-- bag of tuples with each containing a single triple as an NTriples string
rdf_triples = FOREACH propertyGraphWithMappings GENERATE FLATTEN(RDF(*));

-- Write out the output
STORE rdf_triples INTO '/tmp/rdf_triples' USING PigStorage();
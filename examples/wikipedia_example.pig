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
* with the Link-Page graph created from the Wikipedia dataset
*/

REGISTER target/graphbuilder-2.0-alpha-with-deps.jar
IMPORT 'pig/graphbuilder.pig';

--prepare temp storage that is used by the LOAD_TITAN macro
--the temp storage is required for doing a dummy LOAD/STORE for the 
--MAPREDUCE operator use in that macro
rmf /tmp/empty
fs -mkdir /tmp/empty
rmf /tmp/tmp_store_1;
rmf /tmp/tmp_store_2;

xml_data = LOAD 'examples/data/wiki_single.txt' using com.intel.pig.load.XMLLoader('page') AS (page: chararray);
id_extracted = FOREACH xml_data GENERATE REGEX_EXTRACT(page, '<id>(.*?)</id>', 1) AS (id: chararray), page;
title_extracted = FOREACH id_extracted GENERATE REGEX_EXTRACT(page, '<title>(.*?)</title>', 1) AS (title: chararray), id, page;
text_extracted = FOREACH title_extracted GENERATE REGEX_EXTRACT(page, '<text\\s.*>(.*?)</text>', 1) AS (text: chararray), id, title, page;
links_extracted = FOREACH text_extracted GENERATE RegexExtractAllMatches(page, '\\[\\[(.*?)\\]\\]') AS (links:bag{}), id, title; --extract all links as a bag
links_flattened = FOREACH links_extracted GENERATE id, title, FlattenAsGBString(links) AS flattened_links:chararray;--flatten the bag of links in the format GB can process
final_relation = FOREACH links_flattened GENERATE FLATTEN(CreateRowKey(*)); --assign row keys 

--create GB input table
sh echo "disable 'wiki_table'" | hbase shell
sh echo "drop 'wiki_table'" | hbase shell
sh echo "create 'wiki_table', {NAME=>'features'}" | hbase shell

STORE final_relation INTO 'hbase://wiki_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('features:id features:title features:flattened_links');

--build a directed graph with the --directedEdges argument		
-- -O flag specifies overwriting the input Titan table
-- -F flag specifies to unflatten the links (see links_flattened relation above) during tokenization 
LOAD_TITAN('wiki_table', '"features:title=features:id" "features:flattened_links"', 
                             '--directedEdges "features:title,features:flattened_links,LINKS"',
                           'examples/hbase-titan-conf.xml', '-O -F'); 
 

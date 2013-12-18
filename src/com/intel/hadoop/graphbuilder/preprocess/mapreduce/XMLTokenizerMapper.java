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
package com.intel.hadoop.graphbuilder.preprocess.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.graph.Edge;
import com.intel.hadoop.graphbuilder.graph.Vertex;
import com.intel.hadoop.graphbuilder.preprocess.inputformat.GraphTokenizer;
import com.intel.hadoop.graphbuilder.preprocess.mapreduce.keyvalue.VertexEdgeUnionType;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * The Mapper class parses each input value provided by the {@code InputFormat},
 * and outputs a list of {@code Vertex} and a list of {@code Edge} using a
 * {@code GraphTokenizer}.
 * 
 */
public class XMLTokenizerMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, Text> {

  private static final Logger LOG = Logger.getLogger(XMLTokenizerMapper.class);

  @Override
	  public void configure(JobConf job) {
		  super.configure(job);
		  try {
			  this.tokenizer = (GraphTokenizer) Class
				  .forName(job.get("GraphTokenizer")).newInstance();
			  tokenizer.configure(job);
			  this.valClass = job.getMapOutputValueClass();
			  //mapVal = (VertexEdgeUnionType) valClass.newInstance();
		  } catch (InstantiationException e) {
			  e.printStackTrace();
		  } catch (IllegalAccessException e) {
			  e.printStackTrace();
		  } catch (ClassNotFoundException e) {
			  e.printStackTrace();
		  }

		  //mapKey = new IntWritable();
		  links = new ArrayList<String>();
	  }

  @Override
  public void map(LongWritable key, Text value,
      OutputCollector<Text , Text> out, Reporter arg3)
  throws IOException {

	  //parse(value.toString());
	  //mapKey.clear();
	  //mapVal.clear();

	  DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	  //factory.setNamespaceAware(true);
	  DocumentBuilder builder;
	  try {
		  builder = factory.newDocumentBuilder();
		  Document doc = builder.parse(new InputSource(new StringReader(value.toString())));
		  XPathFactory xfactory = XPathFactory.newInstance();
		  XPath xpath = xfactory.newXPath();
		  String title = new String (xpath.evaluate("//page/title/text()", doc));
		  title.trim();
		  title = title.replaceAll("\\s", "_");

		  String ns = new String (xpath.evaluate("//page/ns/text()", doc));
		  if (ns.isEmpty()) ns = "#F3#";
		  String doc_id = new String (xpath.evaluate("//page/id/text()", doc));
		  if (doc_id.isEmpty()) doc_id = "#F3#";
		  String rev_id = new String (xpath.evaluate("//page/revision/id/text()", doc));
		  if (rev_id.isEmpty()) rev_id = "#F3#";
		  String timestamp = new String (xpath.evaluate("//page/revision/timestamp/text()", doc));
		  if (timestamp.isEmpty()) timestamp = "#F3#";
		  String contr_uname = new String (xpath.evaluate("//page/revision/contributor/username/text()", doc));
		  if (contr_uname.isEmpty()) contr_uname = "#F3#";
		  String contr_id = new String (xpath.evaluate("//page/revision/contributor/id/text()", doc));
		  if (contr_id.isEmpty()) contr_id = "#F3#";
		  String minor = new String (xpath.evaluate("//page/revision/minor/text()", doc));
		  if (minor.isEmpty()) minor = "#F3#";
		  String comment = new String (xpath.evaluate("//page/revision/comment/text()", doc));
		  if (comment.isEmpty()) comment = "#F3#";
		  else comment = comment.replaceAll("\\s", "_");
		  String text = new String (xpath.evaluate("//page/revision/text/text()", doc));
		  if (text != null) {
			  text.trim();
			  parseLinks(text);
			  text = text.replaceAll("\n", " ");
			  text = text.replaceAll("\n\r", " ");
		  }
		  String sha1 = new String (xpath.evaluate("//page/revision/sha1/text()", doc));

		  Iterator<String> iterator = links.iterator();

		  String list_str = new String();

		  while (iterator.hasNext()) {
			  list_str = list_str + "," + iterator.next();
		  }
		  list_str = list_str.replace("[[","");
		  list_str = list_str.replace("]]","");
		  text = text.replace("[[","");
		  text = text.replace("]]","");
		  text = text.replace("{{","");
		  text = text.replace("}}","");
		  comment = comment.replace("[[","");
		  comment = comment.replace("]]","");

		  //mapVal = new Text(title + "\t" + ns + "\t" + rev_id + "\t" + timestamp + "\t" + contr_uname + "\t" + contr_id + "\t" + minor + "\t"
		  //		  + comment + "\t" + text + "\t" + sha1 + "\t" + list_str);
		  mapVal = new Text(title + "#::#" + ns + "#::#" + rev_id + "#::#" + timestamp + "#::#" + contr_uname + "#::#" + contr_id + "#::#" + minor + "#::#"
		  		  + comment + "#::#" + text + "#::#" + sha1 + "#::#" + list_str);

		  doc_id.trim();
		  if (doc_id.length() == 0) doc_id = "1234";

		  mapKey = new Text(doc_id);
		  out.collect(mapKey, mapVal);

	  } catch (ParserConfigurationException e) {
		  e.printStackTrace();
	  } catch (SAXException e) {
		  e.printStackTrace();
	  } catch (IOException e) {
		  e.printStackTrace();
	  } catch (XPathExpressionException e) {
		  e.printStackTrace();
	  }

  }
	  /** This function is taken and modified from wikixmlj WikiTextParser */
  private void parseLinks(String text) {
	  links.clear();
	  Pattern catPattern = Pattern
		  .compile("\\[\\[(.*?)\\]\\]", Pattern.MULTILINE);
	  Matcher matcher = catPattern.matcher(text);
	  while (matcher.find()) {
		  String[] temp = matcher.group(1).split("\\|");
		  if (temp == null || temp.length == 0)
			  continue;
		  String link = temp[0];
		  if (!link.replaceAll("\\s", "").isEmpty() && !link.contains(":")) {
			  links.add(link.replaceAll("\\s", "_"));
		  }
	  }
  }

  private Text mapKey;
  private Text mapVal;
  private GraphTokenizer tokenizer;
  protected Class valClass;
  private List<String> links;

}

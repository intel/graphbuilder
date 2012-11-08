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
package com.intel.hadoop.graphbuilder.demoapps.wikipedia.linkgraph;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.intel.hadoop.graphbuilder.graph.Edge;
import com.intel.hadoop.graphbuilder.graph.Vertex;
import com.intel.hadoop.graphbuilder.preprocess.inputformat.GraphTokenizer;
import com.intel.hadoop.graphbuilder.types.EmptyType;
import com.intel.hadoop.graphbuilder.types.StringType;

public class LinkGraphTokenizer implements
    GraphTokenizer<StringType, EmptyType, EmptyType> {
  private static final Logger LOG = Logger.getLogger(LinkGraphTokenizer.class);

  public LinkGraphTokenizer() throws ParserConfigurationException {
    factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    builder = factory.newDocumentBuilder();
    XPathFactory xfactory = XPathFactory.newInstance();
    xpath = xfactory.newXPath();

    vlist = new ArrayList<Vertex<StringType, EmptyType>>();
    elist = new ArrayList<Edge<StringType, EmptyType>>();
    links = new ArrayList<String>();
  }

  @Override
  public void configure(JobConf job) {
  }

  @Override
  public Class vidClass() {
    return StringType.class;
  }

  @Override
  public Class vdataClass() {
    return EmptyType.class;
  }

  @Override
  public Class edataClass() {
    return EmptyType.class;
  }

  public void parse(String s) {

    try {
      Document doc = builder.parse(new InputSource(new StringReader(s)));
      title = xpath.evaluate("//page/title/text()", doc);
      title = title.replaceAll("\\s", "_");
      id = xpath.evaluate("//page/id/text()", doc);
      String text = xpath.evaluate("//page/revision/text/text()", doc);
      parseLinks(text);
    } catch (SAXException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (XPathExpressionException e) {
      e.printStackTrace();
    }
  }

  public Iterator<Vertex<StringType, EmptyType>> getVertices() {
    vlist.clear();
    vlist.add(new Vertex<StringType, EmptyType>(new StringType(title),
        EmptyType.INSTANCE));
    for (String link : links)
      vlist.add(new Vertex<StringType, EmptyType>(new StringType(link),
          EmptyType.INSTANCE));
    return vlist.iterator();
  }

  @Override
  public Iterator<Edge<StringType, EmptyType>> getEdges() {
    if (links.isEmpty())
      return EmptyIterator.INSTANCE;

    elist.clear();
    Iterator<String> iter = links.iterator();
    while (iter.hasNext()) {
      elist.add(new Edge<StringType, EmptyType>(new StringType(title),
          new StringType(iter.next()), EmptyType.INSTANCE));
    }
    return elist.iterator();
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

  private String id;
  private String title;
  private List<String> links;
  private ArrayList<Vertex<StringType, EmptyType>> vlist;
  private ArrayList<Edge<StringType, EmptyType>> elist;

  private DocumentBuilderFactory factory;
  private DocumentBuilder builder;
  private XPath xpath;

}

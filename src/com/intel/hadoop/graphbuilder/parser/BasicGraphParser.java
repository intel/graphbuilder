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
package com.intel.hadoop.graphbuilder.parser;

import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.intel.hadoop.graphbuilder.graph.Edge;
import com.intel.hadoop.graphbuilder.graph.Vertex;

/**
 * A simple GraphParser which assumes tab separated fields for any input. It
 * also treats all string as valid edgedata and vertexdata. In other words,
 * there should not be comments in the input.
 * 
 * @param <VidType>
 * @param <VertexData>
 * @param <EdgeData>
 */
public class BasicGraphParser<VidType extends WritableComparable<VidType>, VertexData extends Writable, EdgeData extends Writable>
    implements GraphParser<VidType, VertexData, EdgeData> {
  private static final Logger LOG = Logger.getLogger(BasicGraphParser.class);

  @Override
  public boolean isEdgeData(String text) {
    return true;
  }

  @Override
  public boolean isVertexData(String text) {
    return true;
  }

  @Override
  public Edge<VidType, EdgeData> parseEdge(String text,
      FieldParser<VidType> vidparser, FieldParser<EdgeData> edataparser) {
    StringTokenizer tokenizer = new StringTokenizer(text);
    try {
      VidType source = vidparser.getValue(tokenizer.nextToken());
      VidType target = vidparser.getValue(tokenizer.nextToken());
      if (tokenizer.hasMoreTokens()) {
        return new Edge<VidType, EdgeData>(source, target,
            edataparser.getValue(tokenizer.nextToken()));
      } else {
        return new Edge<VidType, EdgeData>(source, target,
            edataparser.getValue(""));
      }
    } catch (NumberFormatException e) {
      e.printStackTrace();
    } catch (NoSuchElementException e) {
      e.printStackTrace();
    }
    LOG.error("Error in parsing line:" + text);
    return null;
  }

  @Override
  public Vertex<VidType, VertexData> parseVertex(String text,
      FieldParser<VidType> vidparser, FieldParser<VertexData> vdataparser) {
    StringTokenizer tokenizer = new StringTokenizer(text);
    try {
      VidType id = vidparser.getValue(tokenizer.nextToken());
      if (tokenizer.hasMoreTokens()) {
        return new Vertex<VidType, VertexData>(id,
            vdataparser.getValue(tokenizer.nextToken()));
      } else {
        return new Vertex<VidType, VertexData>(id, vdataparser.getValue(""));
      }

    } catch (NumberFormatException e) {
      e.printStackTrace();
    } catch (NoSuchElementException e) {
      e.printStackTrace();
    }
    LOG.error("Error in parsing line:" + text);
    return null;
  }

}

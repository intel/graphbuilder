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
package com.intel.hadoop.graphbuilder.test.graph;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

import org.junit.Test;

import com.intel.hadoop.graphbuilder.graph.VertexRecord;
import com.intel.hadoop.graphbuilder.types.IntType;
import com.intel.hadoop.graphbuilder.types.StringType;

/**
 * Unit test for VertexRecord.
 *
 * @author Haijie Gu
 *
 */
public class VertexRecordTest {

  @Test
  public void testEncodeInt() {
    int numProcs = 8;
    ArrayList<Integer> mirrorList = new ArrayList<Integer>(Arrays.asList(0, 1,
        2, 3, 4));
    VertexRecord<IntType, IntType> vrec = new VertexRecord<IntType, IntType>();
    vrec.setVid(new IntType(1));
    vrec.setMirrorsFromList(mirrorList, numProcs);
    vrec.setOwner((short) 2);
    vrec.setInEdges(3);
    vrec.setOutEdges(2);
    // System.out.println(vrec.toString());
  }

  @Test
  public void testDecodeInt() {
    int numProcs = 8;
    ArrayList<Integer> mirrorList = new ArrayList<Integer>(Arrays.asList(0, 1,
        2, 3, 4));
    VertexRecord<IntType, IntType> vrec = new VertexRecord<IntType, IntType>();
    vrec.setVid(new IntType(442));
    vrec.setMirrorsFromList(mirrorList, numProcs);
    vrec.setOwner((short) 2);
    vrec.setInEdges(3);
    vrec.setOutEdges(2);
    String s = vrec.toString();
    JSONParser parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE);
    try {
      JSONObject obj = (JSONObject) parser.parse(s);
      // System.out.println(obj.toJSONString());
    } catch (ParseException e) {
      e.printStackTrace();
    }

    s = "{\"mirrors\":[0,1,2,3,4,5,6,7],\"inEdges\":212,\"gvid\":0,\"outEdges\":4,\"owner\":5,\"VertexData\":null}";
    try {
      JSONObject obj = (JSONObject) parser.parse(s);
      // System.out.println(obj.toJSONString());
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Test
  public void testEncodeString() {

    int numProcs = 8;
    ArrayList<Short> mirrorList = new ArrayList<Short>();
    mirrorList.add((short) 0);
    mirrorList.add((short) 1);
    mirrorList.add((short) 2);
    mirrorList.add((short) 3);
    mirrorList.add((short) 4);
    VertexRecord<StringType, StringType> vrec = new VertexRecord<StringType, StringType>();
    vrec.setVid(new StringType("testid"));
    vrec.setInEdges(1);
    vrec.setOutEdges(3);
    BitSet bs = new BitSet(numProcs);

    for (int j = 0; j < mirrorList.size(); j++)
      bs.set(mirrorList.get(j));
    vrec.setMirrors(bs);
    vrec.setVdata(new StringType("http://www.intel.com"));

    List<Short> mirrors = vrec.mirrorList();
    assertEquals("mirrorList:", mirrorList, mirrors);

    // System.out.println(vrec.toString());
  }

  /*
   * @Test public void testDecodeString() { GraphTypeFactory.VidType =
   * StringType.class; GraphTypeFactory.VertexDataType = StringType.class;
   * 
   * int numProcs = 8; ArrayList<Integer> mirrors = new
   * ArrayList<Integer>(Arrays.asList(0,1,2,3,4)); VertexRecord<StringType,
   * StringType> vrec =new VertexRecord<StringType, StringType>(new
   * StringType("testid")); vrec.inEdges = 1; vrec.outEdges = 3; BitSet bs = new
   * BitSet(numProcs); for (int j = 0; j < mirrors.size(); j++)
   * bs.set(mirrors.get(j)); vrec.mirrors = bs; vrec.vdata = new
   * StringType("http://www.intel.com");
   * 
   * String vrecjson = vrec.toJSONObj().toJSONString();
   * 
   * try { VertexRecord<StringType, StringType> newvrec = new
   * VertexRecord<StringType,StringType>(); JSONParser parser = new
   * JSONParser(JSONParser.MODE_JSON_SIMPLE);
   * newvrec.parseJSONObj((JSONObject)parser.parse(vrecjson), numProcs);
   * System.out.println(newvrec.toJSONObj().toJSONString());
   * assertEquals("JSON String", vrecjson, newvrec.toJSONObj().toJSONString());
   * } catch (ParseException e) { // TODO Auto-generated catch block
   * e.printStackTrace(); }
   * 
   * 
   * }
   */
}

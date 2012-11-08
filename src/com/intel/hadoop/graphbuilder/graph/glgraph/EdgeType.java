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
package com.intel.hadoop.graphbuilder.graph.glgraph;

public class EdgeType {
  public enum DIR {
    IN, OUT, EMPTY
  };

  public EdgeType() {
    center = -1;
    connected = -1;
    edgeid = -1;
    direction = DIR.EMPTY;
  }

  public EdgeType(int center, int connected, int edgeid, DIR direction) {
    this.center = center;
    this.connected = connected;
    this.direction = direction;
    this.edgeid = edgeid;
  }

  public int source() {
    if (direction == DIR.IN) {
      return connected;
    } else if (direction == DIR.OUT) {
      return center;
    } else {
      return -1;
    }
  }

  public int target() {
    if (direction == DIR.IN) {
      return center;
    } else if (direction == DIR.OUT) {
      return connected;
    } else {
      return -1;
    }
  }

  public int edgeid() {
    return edgeid;
  }

  int center;
  int connected;
  int edgeid;
  DIR direction;
}

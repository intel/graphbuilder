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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.minidev.json.JSONObject;

/**
 * A dense data structure of a sparse matrix: (Compressed Sparse Row)
 * {@link http
 * ://en.wikipedia.org/wiki/Sparse_matrix#Compressed_sparse_row_.28CSR_or_CRS
 * .29}. This class only represents the zero/non-zero structure of the matrix,
 * and the actual entry data, if any, should be stored separately as a list
 * elsewhere.
 * 
 */
public class SparseGraphStruct {
  /**
   * Initialize a n by n empty matrix.
   * 
   * @param n
   *          the size of the square matrix.
   */
  public SparseGraphStruct(int n) {
    rowIndex = new ArrayList<Integer>(Collections.nCopies(n, -1));
    colIndex = new ArrayList<Integer>();
  }

  /**
   * Initialize a n by n matrix, with entries encoded by a source array and
   * target array.
   * 
   * @param n
   *          the size of the square matrix.
   * @param sources
   *          the list of source ids.
   * @param targets
   *          the list of target ids.
   */
  public SparseGraphStruct(int numVertices, List<Integer> sources,
      List<Integer> targets) {
    colIndex = targets;
    rowIndex = new ArrayList<Integer>(numVertices);
    int lastSource = -1;
    int i = 0;
    for (; i < sources.size(); ++i) {
      int source = sources.get(i);
      if (source != lastSource) {
        for (int j = lastSource + 1; j < source; ++j)
          rowIndex.add(-1);
        rowIndex.add(i);
        lastSource = source;
      }
    }
    for (i = lastSource + 1; i < numVertices; i++)
      rowIndex.add(-1);
    assert numVertices == rowIndex.size() : "SparseGraphStruct: numVertices differs from rowIndex";
  }

  /**
   * @return the number of non-zero entries.
   */
  public int numEdges() {
    return colIndex.size();
  }

  /**
   * @return the dimension.
   */
  public int numVertices() {
    return rowIndex.size();
  }

  /**
   * @param row
   * @return the begin column index of the non-zero entry of a given row index.
   */
  public int begin(int row) {
    return rowIndex.get(row);
  }

  /**
   * @param row
   * @return the end column index of the non-zero entry of a given row index.
   */
  public int end(int row) {
    if (rowIndex.get(row) < 0)
      return -1;

    int i = row + 1;
    while (i < numVertices() && rowIndex.get(i) == -1)
      ++i;
    return i < numVertices() ? rowIndex.get(i) : numEdges();
  }

  /**
   * Clear the matrix.
   */
  public void clear() {
    rowIndex.clear();
    colIndex.clear();
  }

  /**
   * @return the internal row representation.
   */
  public List<Integer> getRowIndex() {
    return rowIndex;
  }

  /**
   * @return the internal column representation.
   */
  public List<Integer> getColIndex() {
    return colIndex;
  }

  /**
   * @return the JSON encoding.
   */
  public JSONObject toJSONObj() {
    JSONObject obj = new JSONObject();
    obj.put("rowIndex", rowIndex);
    obj.put("colIndex", colIndex);
    return obj;
  }

  private List<Integer> rowIndex;
  private List<Integer> colIndex;
}

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

import java.util.Comparator;
import java.util.Iterator;

/**
 * A lazy list representation of a collection of edges that satisfy a direction
 * property with respect to a center vertex. In other words, given a center
 * vertex v, the list represents either the incoming or outgoing edges of v
 * depending on the direction variable {@code dir}. The list has a pointer to an
 * {@code SparseGraphStruct} object as its backend storage. The element of the
 * list is an {@code EdgeType} object which contains lazy pointers to the
 * storage used for obtaining the actual data when evaluated.
 * 
 * @see SparseGraphStruct
 * @see EdgeType
 */
public class EdgeList implements Iterable<EdgeType> {

  /** Default constructor. Creates an empty edge list. */
  public EdgeList() {
    this.store = null;
    this.begin = -1;
    this.end = -1;
    this.dir = EdgeType.DIR.EMPTY;
  }

  /**
   * Construct an EdgeList containing all edges in the {@code SparseGraphStruct}
   * that are incoming (outgoing) edges with respect to the given vertex. The
   * direction is determined by the parameter {@code dir}.
   * 
   * @param vid
   *          the center vertex id.
   * @param store
   *          the backend storage of all edges.
   * @param dir
   *          the direction of edges with respect to the center vertex.
   */
  public EdgeList(int vid, SparseGraphStruct store, EdgeType.DIR dir) {
    this.store = store;
    this.center = vid;
    this.begin = store.begin(vid);
    this.end = store.end(vid);
    this.dir = dir;
  }

  /**
   * @return the size of the list.
   */
  public int size() {
    return end - begin;
  }

  /**
   * @return wheter the list is empty.
   */
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public Iterator<EdgeType> iterator() {
    return new EdgeIterator(this);
  }

  /**
   * This iterator iterates lazily over the {@code EdgeList}.
   * 
   */
  public class EdgeIterator implements Iterator<EdgeType> {
    /** Create an the begin iterator of the list. */
    EdgeIterator(EdgeList list) {
      this.list = list;
      this.cur = list.begin;
    }

    @Override
    public final boolean hasNext() {
      return (list != null && cur < list.end);
    }

    @Override
    public final EdgeType next() {
      EdgeType ret = new EdgeType(list.center, list.store.getColIndex()
          .get(cur), cur, list.dir);
      ++cur;
      return ret;
    }

    @Override
    public final void remove() {
      System.err.println("Edge list does not support remove an edge");
    }

    /** The current position index of the iterator. */
    int cur;
    /** The list to be iterate. */
    EdgeList list;
  }

  /**
   * Comparator of two EdgeType based on the numeric ordering of the center of
   * the edge.
   * 
   * @param <EdgeType>
   *          type of the elements to be compared with.
   */
  public class EdgeTypeComparator implements Comparator<EdgeType> {
    @Override
    public final int compare(EdgeType arg0, EdgeType arg1) {
      return ((Integer) (arg0.connected)).compareTo(arg1.connected);
    }
  }

  /** Index of the center vertex in the storage. */
  int center;
  /** The begin and end index indicating the range of the edges. */
  int begin, end;
  /** The direction of the edges with respect to the center vertex. */
  EdgeType.DIR dir;
  /** The back end dense storage of the edges. */
  SparseGraphStruct store;
}

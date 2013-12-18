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
package com.intel.hadoop.graphbuilder.util;

/**
 * Represents a pair of objects.
 * 
 * 
 * @param <L>
 * @param <R>
 */
public class Pair<L, R> {

  /**
   * Construct a pair with left = l and right = r.
   * 
   * @param l
   * @param r
   */
  public Pair(L l, R r) {
    this.l = l;
    this.r = r;
  }

  /**
   * @return the left value.
   */
  public L getL() {
    return l;
  }

  /**
   * @return the right value.
   */
  public R getR() {
    return r;
  }

  /**
   * @param l
   *          the new value for the left value.
   */
  public void setL(L l) {
    this.l = l;
  }

  /**
   * @param r
   *          the new value for the right value.
   */
  public void setR(R r) {
    this.r = r;
  }

  /**
   * @return a reversed pair.
   */
  public Pair<R, L> reverse() {
    return new Pair<R, L>(r, l);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Pair) {
      return ((Pair) obj).l.equals(l) && ((Pair) obj).r.equals(r);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return HashUtil.hashpair(l, r);
  }

  @Override
  public String toString() {
    return "(" + l + "," + r + ")";
  }

  private L l;
  private R r;
}

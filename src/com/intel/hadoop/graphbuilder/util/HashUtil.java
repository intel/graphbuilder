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
 * Implementation of a hash combine from boost.
 * 
 * @author Haijie Gu
 * 
 */
public class HashUtil {

  /**
   * @param o1
   * @param o2
   * @return the hash value of a pair of objects.
   */
  public static int hashpair(Object o1, Object o2) {
    return combine(combine(0, o1), o2);
  }

  private static int combine(long seed, Object val) {
    return (int) (val.hashCode() + 0x9e3779b9 + (seed << 6) + (seed >> 2));
  }

}

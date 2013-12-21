/**
 * Copyright (C) 2013 Intel Corporation.
 *     All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more about this software visit:
 *     http://www.01.org/GraphBuilder
 */
package com.intel.hadoop.graphbuilder.util;

/**
 * Implementation of a hash combine from boost.
 */
public class HashUtil {

    /**
     * Calculate hashcode of a pair of objects.
     * @param o1  An object.
     * @param o2  An object.
     * @return The hash value of a pair of objects.
     */
    public static int hashPair(Object o1, Object o2) {
        return combine(combine(0, o1), o2);
    }

    /**
     * Calculate hashcode of a triple of objects.
     * @param o1  An object.
     * @param o2  An object.
     * @param o3  An object.
     * @return The hash value of a pair of objects.
     */
    public static int hashTriple(Object o1, Object o2, Object o3) {
        return combine(combine(combine(0, o1), o2), o3);
    }

    /**
     * Calculate hashcode of a triple of objects.
     * @param o1  An object.
     * @param o2  An object.
     * @param o3  An object.
     * @param o4 An object.
     * @return The hash value of a pair of objects.
     */
    public static int hashQuad(Object o1, Object o2, Object o3, Object o4) {
        return combine(combine(combine(combine(0, o1), o2), o3), o4);
    }

    /**
     * Combine that hashcode of an object with an long  seed.
     * @param seed Incoming seed value.
     * @param val  An object to be hashed.
     * @return     A hashcode.
     */
    public static int combine(long seed, Object val) {
        return (int) (val.hashCode() + 0x9e3779b9 + (seed << 6) + (seed >> 2));
    }
}

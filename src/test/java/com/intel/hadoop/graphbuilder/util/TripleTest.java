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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;

import org.junit.Test;

public class TripleTest {
    @Test
    public void testGetA() throws Exception {
        Triple<Integer,Integer,Integer> t = new Triple(7, 8, 9);

        int a = t.getA();
        assertEquals(a, 7);
    }

    @Test
    public void testGetB() throws Exception {
        Triple<Integer,Integer,Integer> t = new Triple(7, 8, 9);

        int b = t.getB();
        assertEquals(b, 8);
    }

    @Test
    public void testGetC() throws Exception {
        Triple<Integer,Integer,Integer> t = new Triple(7, 8, 9);

        int c = t.getC();
        assertEquals(c, 9);
    }

    @Test
    public void testSetA() throws Exception {
        Triple<Integer,Integer,Integer> t = new Triple(7, 8, 9);

        t.setA(3);

        int a = t.getA();
        int b = t.getB();
        int c = t.getC();

        assertEquals(a, 3);
        assertEquals(b, 8);
        assertEquals(c, 9);
    }

    @Test
    public void testSetB() throws Exception {
        Triple<Integer,Integer,Integer> t = new Triple(7, 8, 9);

        t.setB(3);

        int a = t.getA();
        int b = t.getB();
        int c = t.getC();

        assertEquals(a, 7);
        assertEquals(b, 3);
        assertEquals(c, 9);
    }

    @Test
    public void testSetC() throws Exception {
        Triple<Integer,Integer,Integer> t = new Triple(7, 8, 9);

        t.setC(3);

        int a = t.getA();
        int b = t.getB();
        int c = t.getC();

        assertEquals(a, 7);
        assertEquals(b, 8);
        assertEquals(c, 3);
    }

    @Test
    public void testSwapAB() throws Exception {
        Triple<Integer,Integer,Integer> t = new Triple(7, 8, 9);

        Triple<Integer,Integer,Integer>  tSwapped = t.swapAB();

        int a = t.getA();
        int b = t.getB();
        int c = t.getC();

        assertEquals(a, 7);
        assertEquals(b, 8);
        assertEquals(c, 9);

        int x = tSwapped.getA();
        int y = tSwapped.getB();
        int z = tSwapped.getC();

        assertEquals(x, 8);
        assertEquals(y, 7);
        assertEquals(z, 9);
    }

    @Test
    public void testEqualsAndHash() throws Exception {
        Triple<Object, Object, Object> tripNulls1 =
                new Triple(null, null, null);
        Triple<Object, Object, Object> tripNulls2 =
                new Triple(null, null, null);

        assert(tripNulls1.equals(tripNulls2));

        Integer intObj1      = new Integer(0);
        Integer intObj2      = new Integer(0);
        Integer intDifferent = new Integer(1);

        Triple<Integer, Integer, Integer> tripInts0  =
                new Triple(intObj1, intObj1, intObj1);
        Triple<Integer, Integer, Integer> tripInts0a =
                new Triple(intObj2, intObj2, intObj2);
        Triple<Integer, Integer, Integer> tripInts1  =
                new Triple(intDifferent, intObj1, intObj1);
        Triple<Integer, Integer, Integer> tripInts2  =
                new Triple(intObj1, intDifferent, intObj1);
        Triple<Integer, Integer, Integer> tripInts3  =
                new Triple(intObj1, intObj1, intDifferent);

        assert(tripInts0.equals(tripInts0a));
        assertFalse(tripInts0.equals((tripInts1)));
        assertFalse(tripInts0.equals((tripInts2)));
        assertFalse(tripInts0.equals((tripInts3)));

        assertFalse(tripInts0.equals(null));

        assertNotNull(tripInts0.hashCode());
        assert(tripInts0.hashCode() == tripInts0a.hashCode());
    }

    @Test
    public void testToString() throws Exception {
        Triple<Integer,Integer,Integer> t      = new Triple(7, 8, 9);
        Triple<Object, Object, Object>  tNulls = new Triple(null, null, null);

        assert(t.toString().compareTo(new String("(7, 8, 9)")) == 0);
        assert(tNulls.toString().compareTo(new String("(null, null, null)"))
                == 0);
    }
}

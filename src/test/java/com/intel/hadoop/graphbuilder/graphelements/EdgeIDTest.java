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
package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.util.Triple;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertSame;

public class EdgeIDTest {
    @Test
    public final void testGetA() throws Exception {
        Triple<Integer, Integer, Integer> t = new Triple(7, 8, 9);

        int a = t.getA();
        assertEquals(a, 7);
    }

    @Test
    public final void testConstructorGet() {
        Object src = new String("src");
        Object dst = new String("dst");
        Object label = new String("label");

        EdgeID edgeID = new EdgeID(src, dst, label);

        assertNotNull(edgeID);
        assertSame(edgeID.getSrc(), src);
        assertSame(edgeID.getDst(), dst);
        assertSame(edgeID.getLabel(), label);
    }

    @Test
    public final void testConstructorGetSet() {
        Object src = new String("src");
        Object dst = new String("dst");
        Object label = new String("label");

        Object one = new Integer(0);
        Object two = new Integer(1);
        Object three = new Integer(2);

        EdgeID edgeID = new EdgeID(src, dst, label);

        assertNotNull(edgeID);
        assertSame(edgeID.getSrc(), src);
        assertSame(edgeID.getDst(), dst);
        assertSame(edgeID.getLabel(), label);

        edgeID.setSrc(one);

        assertSame(edgeID.getSrc(), one);
        assertSame(edgeID.getDst(), dst);
        assertSame(edgeID.getLabel(), label);

        edgeID.setDst(two);

        assertSame(edgeID.getSrc(), one);
        assertSame(edgeID.getDst(), two);
        assertSame(edgeID.getLabel(), label);

        edgeID.setLabel(three);

        assertSame(edgeID.getSrc(), one);
        assertSame(edgeID.getDst(), two);
        assertSame(edgeID.getLabel(), three);

        edgeID.setLabel(label);

        assertSame(edgeID.getSrc(), one);
        assertSame(edgeID.getDst(), two);
        assertSame(edgeID.getLabel(), label);

        edgeID.setDst(dst);

        assertSame(edgeID.getSrc(), one);
        assertSame(edgeID.getDst(), dst);
        assertSame(edgeID.getLabel(), label);

        edgeID.setSrc(src);

        assertSame(edgeID.getSrc(), src);
        assertSame(edgeID.getDst(), dst);
        assertSame(edgeID.getLabel(), label);
    }

    @Test
    public final void testReverseEdge() {
        Object src = new String("src");
        Object dst = new String("dst");
        Object label = new String("label");

        EdgeID edgeID = new EdgeID(src, dst, label);

        assertNotNull(edgeID);
        assertSame(edgeID.getSrc(), src);
        assertSame(edgeID.getDst(), dst);
        assertSame(edgeID.getLabel(), label);

        EdgeID reverseEdgeID = edgeID.reverseEdge();
        assertSame(reverseEdgeID.getSrc(), dst);
        assertSame(reverseEdgeID.getDst(), src);
        assertSame(reverseEdgeID.getLabel(), label);

        EdgeID reverseReverseEdgeID = edgeID.reverseEdge().reverseEdge();
        assertSame(reverseReverseEdgeID.getSrc(), src);
        assertSame(reverseReverseEdgeID.getDst(), dst);
        assertSame(reverseReverseEdgeID.getLabel(), label);
    }

    @Test
    public final void testEquals() {
        Object src = new String("src");
        Object badSrc = new String("badSrc");
        Object dst = new String("dst");
        Object badDst = new String("badDst");
        Object label = new String("label");
        Object badLabel = new String("I hate labels");

        EdgeID edgeID = new EdgeID(src, dst, label);
        EdgeID edgeIDSame = new EdgeID(src, dst, label);
        EdgeID edgeIDDelta1 = new EdgeID(badSrc, dst, label);
        EdgeID edgeIDDelta2 = new EdgeID(src, badDst, label);
        EdgeID edgeIDDelta3 = new EdgeID(src, dst, badLabel);

        assert (edgeID.equals(edgeIDSame));
        assertFalse(edgeID.equals(edgeIDDelta1));
        assertFalse(edgeID.equals(edgeIDDelta2));
        assertFalse(edgeID.equals(edgeIDDelta3));

        // can't forget this one
        assertFalse(edgeID.equals(null));
    }

    @Test
    public final void testHashCode() {
        Object src = new String("src");
        Object dst = new String("dst");
        Object label = new String("label");

        EdgeID edgeID = new EdgeID(src, dst, label);

        int hash = edgeID.hashCode();

        assertNotNull(hash);
    }

    @Test
    public final void testToString() {
        Object src = new String("src");
        Object dst = new String("dst");
        Object label = new String("label");

        Object src2 = new String("src");
        Object dst2 = new String("dst");
        Object label2 = new String("label");

        EdgeID edgeID1 = new EdgeID(src, dst, label);
        EdgeID edgeID2 = new EdgeID(src2, dst2, label2);

        String toString1 = edgeID1.toString();
        String toString2 = edgeID2.toString();

        assertNotNull(toString1);
        assertFalse(toString1.compareTo("") == 0);

        assert (toString2.compareTo(toString1) == 0);

        // all those wonderful negative tests

        Object badSrc = new String("badSrc");
        Object badDst = new String("badDst");
        Object badLabel = new String("badLabel");

        EdgeID edgeIDDelta1 = new EdgeID(badSrc, dst, label);
        EdgeID edgeIDDelta2 = new EdgeID(src, badDst, label);
        EdgeID edgeIDDelta3 = new EdgeID(src, dst, badLabel);

        assertFalse(toString1.compareTo(edgeIDDelta1.toString()) == 0);
        assertFalse(toString1.compareTo(edgeIDDelta2.toString()) == 0);
        assertFalse(toString1.compareTo(edgeIDDelta3.toString()) == 0);
    }
}

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
package com.intel.hadoop.graphbuilder.types;

import static junit.framework.Assert.assertEquals;

import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class PropertyMapTest {
    @Test
    public void testSetProperty() throws Exception {
        PropertyMap pm= new PropertyMap();

        IntWritable    one = new IntWritable(1);
        IntWritable    two = new IntWritable(2);
        DoubleWritable pi  = new DoubleWritable(3.14159);

        pm.setProperty("p1", one);
        pm.setProperty("p2", pi);
        pm.setProperty("p1", two);

        // assert for object equality, the maps should not replace objects
        assertEquals(pm.getProperty("p1"), two);
        assertEquals(pm.getProperty("p2"), pi);
    }

    @Test
    public void testRemoveProperty() throws Exception {
        PropertyMap pm= new PropertyMap();

        IntWritable    one = new IntWritable(1);
        DoubleWritable pi  = new DoubleWritable(3.14159);

        pm.setProperty("p1", one);
        pm.setProperty("p2", pi);

        assertEquals(pm.getProperty("p2"), pi);

        pm.removeProperty("p2");

        assertEquals(pm.getProperty("p1"), one);
        assertEquals(pm.getProperty("p2"), null);
    }

    @Test
    public void testGetProperty() throws Exception {
        PropertyMap pm= new PropertyMap();

        assertEquals(pm.getProperty("foo"), null);



        IntWritable    one = new IntWritable(1);
        IntWritable    two = new IntWritable(2);
        DoubleWritable pi  = new DoubleWritable(3.14159);

        // assert for object equality, the maps should not replace objects
        pm.setProperty("foo", one);
        assertEquals(pm.getProperty("foo"), one);
        pm.setProperty("foo", two);
        assertEquals(pm.getProperty("foo"), two);
        pm.setProperty("foo", pi);
        assertEquals(pm.getProperty("foo"), pi);
    }

    @Test
    public void testGetPropertyKeys() throws Exception {
        PropertyMap pm= new PropertyMap();

        assert(pm.getPropertyKeys().isEmpty());

        IntWritable    one = new IntWritable(1);
        IntWritable    two = new IntWritable(2);
        DoubleWritable pi  = new DoubleWritable(3.14159);

        pm.setProperty("p1", one);
        pm.setProperty("p2", pi);
        pm.setProperty("p1", two);

        Set<Writable> keySet = pm.getPropertyKeys();

        for (Writable key : keySet)
        {
            assert(key.toString().compareTo("p1") == 0 || key.toString().compareTo("p2") == 0);
        }

        boolean foundP1 = false;
        boolean foundP2 = false;

        for (Writable key : keySet)
        {
            foundP1 |= (key.toString().compareTo("p1") == 0);
            foundP2 |= (key.toString().compareTo("p2") == 0);
        }

        assert(foundP1);
        assert(foundP2);
    }

}

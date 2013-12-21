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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

import com.intel.hadoop.graphbuilder.graphelements.GraphElement;

public class ArgumentBuilderTest {

    private static final String testArgument = "zombies";
    private static final String testArgumentValue = "like brains";

    private ArgumentBuilder args;

    @After
    public void tearDown(){
        args = null;
    }

    @Test
    public void test_setting_and_getting(){
        args = new ArgumentBuilder().with(testArgument, testArgumentValue);

        assertTrue("verify exists with arg name", args.exists(testArgument));
        assertTrue("verify exists with arg object", args.exists((Object)testArgumentValue));
        assertTrue("verify type matches", args.isType(testArgument, String.class));
        assertEquals("verify set value and the get value", args.get(testArgument), testArgumentValue);

        args = ArgumentBuilder.newArguments().with(testArgument, testArgumentValue);

        assertTrue("verify exists with arg name", args.exists(testArgument));
        assertTrue("verify exists with arg object", args.exists((Object)testArgumentValue));
        assertTrue("verify type matches", args.isType(testArgument, String.class));
        assertEquals("verify set value and the get value", args.get(testArgument), testArgumentValue);
    }

    @Test
    public void test_isEmpty(){
        args = new ArgumentBuilder();
        assertFalse("This value should not exist", args.exists("some arg name"));
        assertFalse("This value should not exist", args.exists((Object)testArgumentValue));

        args.with(testArgument, testArgumentValue);
        assertTrue("argument should exist", args.exists(testArgument));
        assertTrue("argument should exist", args.exists((Object)testArgumentValue));


        args = ArgumentBuilder.newArguments();
        assertFalse("This value should not exist", args.exists("some arg name"));
        assertFalse("This value should not exist", args.exists((Object)testArgumentValue));

        args = ArgumentBuilder.newArguments().with(testArgument, testArgumentValue);
        assertTrue("argument should exist", args.exists(testArgument));
        assertTrue("argument should exist", args.exists((Object) testArgumentValue));
    }

    @Test
    public void test_without(){
        args = new ArgumentBuilder();
        args.with(testArgument, testArgumentValue);

        verifyExistsArgument();

        args.withOut(testArgument);
        verifyDoesntExistsArgument();

        args = ArgumentBuilder.newArguments().with(testArgument, testArgumentValue);

        verifyExistsArgument();

        args.withOut(testArgument);
        verifyDoesntExistsArgument();
    }

    @Test
    public void test_empty(){
        args = new ArgumentBuilder();
        assertTrue("we should have an empty arg set", args.isEmpty());

        args = ArgumentBuilder.newArguments();
        assertTrue("we should have an empty arg set", args.isEmpty());
    }

    @Test
    public void test_get_with_default_value(){
        args = new ArgumentBuilder();
        assertTrue("make sure we get the default value when empty", (args.get(testArgument, Boolean.TRUE) == Boolean.TRUE));
        args.with(testArgument, testArgumentValue);
        assertTrue("make sure we get the argument builder value when set and no the default value",
                (args.get(testArgument, Boolean.TRUE) == testArgumentValue));


        args = ArgumentBuilder.newArguments();
        assertTrue("make sure we get the default value when empty", (args.get(testArgument, Boolean.TRUE) == Boolean.TRUE));
        args.with(testArgument, testArgumentValue);
        assertTrue("make sure we get the argument builder value when set and no the default value",
                (args.get(testArgument, Boolean.TRUE) == testArgumentValue));

    }

    private void verifyExistsArgument(){
        assertTrue("verify exists with arg name", args.exists(testArgument));
        assertTrue("verify exists with arg object", args.exists((Object)testArgumentValue));
        assertTrue("verify type matches", args.isType(testArgument, testArgumentValue.getClass()));
        //verify against any other class to make sure it's not always returning true
        assertFalse("verify type doesn't match", args.isType(testArgument, GraphElement.class));
    }

    private void verifyDoesntExistsArgument(){
        assertFalse("verify exists with arg name", args.exists(testArgument));
        assertFalse("verify exists with arg object", args.exists((Object) testArgumentValue));
    }

}

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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;


@RunWith(PowerMockRunner.class)
@PrepareForTest(RuntimeConfig.class)
public class RunTimeConfigTest {
    private static final String testLoadingThroughReflection = "ahh you found me";
    private static final HashMap<String, String> config = new HashMap<String, String>();

    static {
        config.put("VIDMAP_HTABLE_NAME", "default param");
        config.put("HBASE_CACHE_SIZE", "500");
    }

    private static final String propertiesFileName = "titan-hbase-config";

    private RuntimeConfig runtimeConfig;
    private RuntimeConfig mockedRuntimeConfig;
    private RuntimeConfig spiedRuntime;

    @Before
    public final void setUp() {
        mockedRuntimeConfig = mock(RuntimeConfig.class);
        runtimeConfig = RuntimeConfig.getInstance();
        spiedRuntime = PowerMockito.spy(runtimeConfig);

    }

    @After
    public final void tearDown() {
        mockedRuntimeConfig = null;
        Whitebox.setInternalState(RuntimeConfig.class, "uniqueInstanceOfRuntimeConfig", mockedRuntimeConfig);
        runtimeConfig = null;
        spiedRuntime = null;
    }

    @Test
    public final void verify_unique_instance() {

        RuntimeConfig runtimeConfig1 = RuntimeConfig.getInstance();

        RuntimeConfig runtimeConfig2 = RuntimeConfig.getInstance(RunTimeConfigTest.class);


        HashMap<String, String> config1 = Whitebox.getInternalState(runtimeConfig1, "config");
        HashMap<String, String> config2 = Whitebox.getInternalState(runtimeConfig2, "config");

        assertEquals("make sure the config of both runtimes is equal", config1, config2);
    }

    @Test
    public final void verify_unique_instance_class_specific_instantiation_first() {
        mockedRuntimeConfig = null;
        Whitebox.setInternalState(RuntimeConfig.class, "uniqueInstanceOfRuntimeConfig", mockedRuntimeConfig);

        Whitebox.setInternalState(spiedRuntime, "config", new HashMap<String, String>());
        RuntimeConfig runtimeConfig1 = RuntimeConfig.getInstance(RunTimeConfigTest.class);

        RuntimeConfig runtimeConfig2 = RuntimeConfig.getInstance();


        HashMap<String, String> config1 = Whitebox.getInternalState(runtimeConfig1, "config");
        HashMap<String, String> config2 = Whitebox.getInternalState(runtimeConfig2, "config");

        assertEquals("make sure the config of both runtimes is equal", config1, config2);
    }

    @Test
    public final void verify_grabbing_static_class_fields() {
        mockedRuntimeConfig = null;
        Whitebox.setInternalState(RuntimeConfig.class, "uniqueInstanceOfRuntimeConfig", mockedRuntimeConfig);

        RuntimeConfig staticTest = RuntimeConfig.getInstance(RunTimeConfigTest.class);

        HashMap<String, String> privateConfig = Whitebox.getInternalState(staticTest, "config");
        assertEquals("verify the config size", 3, privateConfig.size());
        assertEquals("verify the config value equals our static variable", testLoadingThroughReflection,
                privateConfig.get("testLoadingThroughReflection"));
        assertEquals("verify the config value equals our static variable", config.toString(),
                privateConfig.get("config"));
    }

    @Test
    public final void test_loading_grahbbuilder_config_namespace() {
        Configuration conf = new Configuration();
        conf.set("graphbuilder.namespace.ValueKeyOne", "one");
        conf.set("graphbuilder.namespace.ValueKeyTwo", "two");
        conf.set("graphBuilder.namespace.ValueKeyThree", "three");

        spiedRuntime.loadConfig(conf);

        assertTrue("verify value key one", spiedRuntime.getProperty("NAMESPACE_VALUEKEYONE").equals("one"));
        assertTrue("verify value key two", spiedRuntime.getProperty("NAMESPACE_VALUEKEYTWO").equals("two"));
        assertTrue("verify value key three is false name space is incorrect",
                spiedRuntime.getProperty("NAMESPACE_VALUEKEYTHREE") == null);
        System.out.print(true);
    }

    @Test
    public final void verify_getPropertyInt() {
        Configuration conf = new Configuration();
        conf.set("graphbuilder.namespace.ValueKeyOne", "1");

        spiedRuntime.loadConfig(conf);

        assertTrue("verify value key one", spiedRuntime.getPropertyInt("NAMESPACE_VALUEKEYONE") == 1);
    }

    @Test
    public final void test_loading_config_key_parsing() {
        Configuration conf = new Configuration();
        conf.set("graphbuilder.namespace.ValueKeyOne", "one");
        conf.set("graphbuilder.namespace_ValueKeyTwo", "two");
        conf.set("graphbuilder.name.space.Value.Key.Three", "three");

        spiedRuntime.loadConfig(conf);

        assertTrue("verify value key one", spiedRuntime.getProperty("NAMESPACE_VALUEKEYONE").equals("one"));
        assertTrue("verify value key two", spiedRuntime.getProperty("NAMESPACE_VALUEKEYTWO").equals("two"));
        assertTrue("verify value key three",
                spiedRuntime.getProperty("NAME_SPACE_VALUE_KEY_THREE").equals("three"));
    }
}

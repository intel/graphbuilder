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

import com.intel.hadoop.graphbuilder.types.LongType;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class ValueClassFactoryTest {
    @Test
    public void testGetValueClassByVidClassName() throws Exception {

        assertSame(ValueClassFactory.getValueClassByVidClassName(StringType.class.getName()),
                SerializedGraphElementStringTypeVids.class);

        assertSame(ValueClassFactory.getValueClassByVidClassName(LongType.class.getName()),
                SerializedGraphElementLongTypeVids.class);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testIllegalArgumentException() {
        // String is not an allowed class because it's not a Writable...
        ValueClassFactory.getValueClassByVidClassName(String.class.getName());
    }

    @Test public void testConstructor() {

        // nothing to do here yet, really a placeholder
        ValueClassFactory vcf = new ValueClassFactory();
        assertNotNull(vcf);
    }
}

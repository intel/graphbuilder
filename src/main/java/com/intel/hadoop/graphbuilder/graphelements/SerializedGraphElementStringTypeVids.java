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

import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.io.WritableComparable;

/**
 * Serialized form of GraphElement class that uses {@code StringType} vertex names in its vertex IDs.
 *
 * We have this class because in Java, a parameterized type cannot be instantiated.
 * In particular, we cannot express {@code createVid} inside {@code SerializedGraphElement}
 */

public class SerializedGraphElementStringTypeVids
        extends SerializedGraphElement<StringType> {

    /**
     * Allocates a new vertex ID.
     * @return  A new {@code StringType} object.
     *
     */

    public VertexID<StringType> createVid() {
        return new VertexID<StringType>(new StringType(), new StringType());
    }
}

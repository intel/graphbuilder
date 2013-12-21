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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Basic type in the library. Represents a dummy type of empty.
 */
public class EmptyType implements Writable, Mergeable<EmptyType>, EncapsulatedObject {

    public final static EmptyType INSTANCE = new EmptyType();

    @Override
    public void readFields(DataInput arg0) throws IOException {
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
    }

    @Override
    public String toString() {
        return "";
    }

    @Override
    public void add(EmptyType other) {
    }

    public Object getBaseObject() {
        return INSTANCE;
    }
}

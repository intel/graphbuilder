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

import net.minidev.json.JSONAware;
import net.minidev.json.JSONValue;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The basic int type in the library.
 */
public class IntType implements WritableComparable<IntType>, Mergeable<IntType>, JSONAware, EncapsulatedObject {

    public static final IntType ZERO = new IntType(0);
    public static final IntType ONE  = new IntType(1);

    public IntType() {
        val = 0;
    }

    public IntType(int i) {
        val = i;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        val = arg0.readInt();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeInt(val);
    }

    @Override
    public String toString() {
        return String.valueOf(val);
    }

    @Override
    public int compareTo(IntType arg0) {
        return ((Integer) val).compareTo(arg0.val);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntType) {
            return ((IntType) obj).val == val;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return val;
    }

    public Object getBaseObject() {
        return val;
    }

    public int get() {
        return val;
    }

    public void set(int i) {
        val = i;
    }

    private int val;

    @Override
    public void add(IntType other) {
        val += other.val;
    }

    @Override
    public String toJSONString() {
        return JSONValue.toJSONString(val);
    }
}

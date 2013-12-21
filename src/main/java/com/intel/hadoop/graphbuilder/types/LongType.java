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
 * The basic long type in the library.
 */
public class LongType implements WritableComparable<LongType>, Mergeable<LongType>, JSONAware, EncapsulatedObject {

    public LongType() {
        this.val = 0;
    }

    public LongType(long i) {
        this.val = i;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        val = arg0.readLong();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(val);
    }

    @Override
    public int compareTo(LongType o) {
        return ((Long) val).compareTo(o.val);
    }

    @Override
    public boolean equals(Object obj) {

        if (obj instanceof LongType) {
            return ((LongType) obj).val == val;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return ((Long) val).hashCode();
    }

    @Override
    public String toString() {
        return String.valueOf(val);
    }

    public long get() {
        return val;
    }

    public Object getBaseObject() {
        return val;
    }

    public void set(long i) {
        val = i;
    }

    private long val;

    @Override
    public void add(LongType other) {
        val += other.val;
    }

    @Override
    public String toJSONString() {
        return JSONValue.toJSONString(val);
    }
}

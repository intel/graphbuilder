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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The basic string type in the library.
 */
public class StringType implements WritableComparable<StringType>, EncapsulatedObject, JSONAware {

    private String str;

    public StringType() {
        this.str = "";
    }

    public StringType(String str) {
        this.str = str;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        Text text = new Text();
        text.readFields(arg0);
        this.str = text.toString();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        new Text(str).write(arg0);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof StringType) && ((StringType) obj).str.equals(str);
    }

    @Override
    public int hashCode() {
        return str.hashCode();
    }

    @Override
    public String toString() {
        return this.str;
    }

    public String get() {
        return this.str;
    }

    public Object getBaseObject() {
        return this.str;
    }

    public void set(String s) {
        this.str = s;
    }

    public boolean isEmpty() {
        return (this.str.length() == 0);
    }

    @Override
    public int compareTo(StringType arg0) {
        return str.compareTo(arg0.str);
    }

    @Override
    public String toJSONString() {
        return JSONValue.toJSONString(str);
    }
}

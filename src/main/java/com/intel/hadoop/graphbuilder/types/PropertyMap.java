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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Map;
import java.util.Set;

import com.intel.hadoop.graphbuilder.util.HashUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;

/**
 *  A serialized property map.
 *
 * This class is mutable.  It wraps a  {@code MapWritable} and adds a friendly toString() method.
 */
public class PropertyMap implements Writable
{
    private MapWritable properties = null;

    /**
     * Default constructor. Allocates the underlying {@code MapWritable}.
     */
    public PropertyMap() {
        this.properties = new MapWritable();
    }

    /**
     * Sets a key value pair in the property map.
     * @param key  Key at which value is to be inserted.
     * @param value Value to be inserted.
     */
    public void setProperty(final String key, final Writable value) {
        StringType keyStr = new StringType(key);
        this.properties.put(keyStr, value);
    }

    /**
     * Given a key, removes the key-value pair from the map if the key is present. No effect if the key is not present.
     * @param key The key whose value is to be removed.
     * @return  The value previously associated with the key (if there was one),
     * null if no value was associated with the key.
     */
    public Writable removeProperty(final String key) {
        StringType keyStr = new StringType(key);
        return properties.remove(keyStr);
    }

    /**
     * Get the value associated with a key.
     * @param key  Key whose value is being queried.
     * @return The value associated with the key, null if no value is associated with the key.
     */
    public Writable getProperty(final String key) {
        StringType keyStr = new StringType(key);
        return this.properties.get(keyStr);
    }

    /**
     * @return The set of all keys associated with values in this map.
     */
    public Set<Writable> getPropertyKeys() {
        return this.properties.keySet();
    }

    /**
     * Incorporate the key-value pairs of an incoming {@code PropertyMap} into this {@code PropertyMap}
     *
     * If the incoming {@code PropertyMap} conflicts with this {@code PropertyMap},the value from the incoming
     * map overrides the value previously in this map.
     *
     * @param propertyMap incoming property map.
     */
    public void mergeProperties(PropertyMap propertyMap) {
        for(Writable key : propertyMap.getPropertyKeys()) {
            this.setProperty(key.toString(), propertyMap.getProperty(key.toString()));
        }
    }

    /**
     * Property maps are equal if and only if they map exactly the same keys to exactly the same values.
     *
     * @param object
     * @return
     */
    public boolean equals(Object object) {
        if (object instanceof PropertyMap) {
            PropertyMap inPropMap = (PropertyMap) object;
            if (inPropMap.getPropertyKeys().size() == this.getPropertyKeys().size()) {
                for (Writable key : this.getPropertyKeys()) {
                    String stringKey = key.toString();
                    Writable value = inPropMap.getProperty(stringKey);
                    if (!value.equals(this.getProperty(stringKey))) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * Override of the {@code hashCode} method to be consistent with the overridden {@code equals} method.
     *
     * Hashcode values determined only by the hashes of the key/value pairs in the map.
     * @return int hashcode
     */
    @Override
    public int hashCode() {

        int tempHash = 0;

        for (Writable key : this.getPropertyKeys()) {
            tempHash = HashUtil.combine(tempHash, key);
            Writable value = this.getProperty(key.toString());
            tempHash = HashUtil.combine(tempHash, value);
        }

        return tempHash;
    }

    /**
     * Read the {@code PropertyMap} from a data stream.
     *
     * @param in  The input data stream.
     *
     * @throws IOException
     */
    @Override
    public  void readFields(DataInput in) throws IOException {
        this.properties.readFields(in);
    }

    /**
     * Write the {@code PropertyMap} to a data stream.
     * @param out The output data stream.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        this.properties.write(out);
    }

    /**
     * Convert the {@code PropertyMap} to a {@code String}
     *
     * @return String representation of the {@code PropertyMap}
     */

    @Override
    public String toString() {

        StringBuffer s = new StringBuffer();

        if (!properties.isEmpty())
        {

            for (Map.Entry<Writable, Writable> entry : properties.entrySet())
            {
               s.append(entry.getKey() + ":" + entry.getValue() + "\t");
            }

        }

        return s.toString();
    }
}

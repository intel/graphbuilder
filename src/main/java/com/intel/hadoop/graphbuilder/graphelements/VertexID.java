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
import com.intel.hadoop.graphbuilder.util.HashUtil;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * The identifying information of a vertex: Its name, and its  its label.
 *
 * <p>
 * It is used in the initial reducer of an {@code GraphGenerationJob}to detect and handle duplicate edges.
 * Edges are considered duplicate if they have identical source, destination and label.
 * Property maps are <i>not</i> used for purposes of comparison.
 *    </p>
 * @see com.intel.hadoop.graphbuilder.pipeline.output.titan.EdgesIntoTitanReducer
 */
public class VertexID<VidType extends WritableComparable> implements WritableComparable<VertexID<VidType>> {

    private StringType label = null;
    private VidType    name  = null;

    /**
     * Default, no argument constructor.
     */
    private VertexID() {
    }

    /**
     * Construct {@code VertexID} from parameters.
     * @param name The {@code VidType} vertex name
     * @param label  The {@code String} vertex label
     */
    public VertexID(VidType name, StringType label) {
        this.label = label;
        this.name  = name;
    }
    /**
     * Construct {@code VertexID} from parameters.
     * @param name The {@code VidType} vertex name
     */
    public VertexID(VidType name) {
        this.name  = name;
        this.label = null;
    }
    /**
     * @return The label of the vertex.
     */
    public StringType getLabel() {
        return this.label;
    }

    /**
     * Set the vertex label.
     * @param label  the new vertex label
     */
    public void setLabel(StringType label) {
        this.label = label;
    }

    /**
     * @return The name of the vertex.
     */
    public VidType getName() {
        return this.name;
    }

    /**
     * Set the name of the vertex.
     * @param name
     */
    public void setName(VidType name) {
        this.name = name;
    }
    /**
     * Equality test: Is the other object a {@code VertexID} whose components are equal to those of this vertexID?
     *
     * Because the {@code equals} method of the underlying {@code StringType} type for {@code label} and {@code name}
     * depends only upon the equality of the underlying strings, the {@code equals} method {@code VertexID} depends only
     * upon the string equality of the labels and names.
     * @param obj  Any object.
     * @return   {@literal true} if the object is a {@VertexID} of value equal to the current {@code VertexID},
     * {@literal false} otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VertexID) {
            VertexID k = (VertexID) obj;
            return (Objects.equals(this.label, k.label)
                    && Objects.equals(this.name, k.name));
        }
        return false;
    }

    /**
     * Hash of the VertexID is determined by the label/name pair as {@code StringType} objects.
     *
     * Because hash codes of {@code StringType} objects depend only upon underlying string value, the hashcode of a
     * {@code VertexID} depends only on the underlying string values of its label and name.
     *
     * @return  hash of the VertexID
     * @see StringType
     */
    @Override
    public int hashCode() {
        if (label == null && name == null) {
            return 0xdeadbeef;
        } else if (label == null && name != null) {
            return name.hashCode();
        } else if (label != null && name == null) {
            return label.hashCode();
        } else {
            return HashUtil.hashPair(label, name);
        }
    }

    /**
     * compareTo implementation simply calls the equals method
     * @param vertexID
     * @return
     */
    public int compareTo(VertexID<VidType> vertexID) {
        return equals(vertexID) ? 0 : 1;
    }

    /**
     * @return a string representation of the {@code VertexID}
     */
    @Override
    public String toString() {
        if (label == null && name == null) {
            return "null vertex";
        } else if (label == null && name != null) {
            return this.name.toString();
        } else if (label != null && name == null) {
            return "null vertex with label " + label;
        } else {
            return label + "." + name.toString();
        }
    }

    /**
     * Reads an edge from an input stream.
     * @param input The input stream.
     * @throws java.io.IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {
        boolean hasLabel = input.readBoolean();

        name.readFields(input);

        if (hasLabel) {
            label.readFields(input);
        }  else {
            label = null;
        }
    }

    /**
     * Writes an edge to an output stream.
     * @param output The output stream.
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {
        boolean hasLabel = (label != null);

        output.writeBoolean(hasLabel);

        name.write(output);

        if (hasLabel) {
            label.write(output);
        }
    }
}

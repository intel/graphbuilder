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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * The serialized wrapper class for {@code GraphElement}.
 *
 * @param {@code <VidType>}  The vertex ID type.
 * @see GraphElement
 */

public abstract class SerializedGraphElement<VidType extends WritableComparable<VidType>>
        implements Writable, WritableComparable<Object> {


    private GraphElement graphElement;

    /**
     *  Allocates new object. The wrapped {@code GraphElement} is initialized to {@code null}.
     */
    public SerializedGraphElement() {
        this.graphElement = null;
    }

    /**
     *  Allocates new object. Wrapped {@code GraphElement} is initialized to input parameter.
     *  @param graphElement Value to initialize the wrapped graphElement
     */
    public SerializedGraphElement(GraphElement graphElement) {
        this.graphElement = graphElement;
    }

    /**
     * Allocates a new vertex ID object.
     * @return  A new object of type {@code VidType}.
     */
    public abstract VertexID<VidType> createVid();

    /**
     * Passes in a {@code graphElement} to be wrapped.
     *
     * @param {@code graphElement}  The {@code graphElement} to be wrapped.
     */
    public void init(GraphElement graphElement) {

        this.graphElement = graphElement;
    }


    /**
     * @return  The base {@code GraphElement}.
     */

    public GraphElement graphElement() {
        if (this.graphElement == null)
            return null;
        return this.graphElement.get();
    }

    /**
     * Reads the {@code SerializedGraphElement} from an input stream.
     * @param {@code input}  The input stream.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {

        boolean isVertex = input.readBoolean();

        if (isVertex) {

            Vertex<VidType> vertex = new Vertex<VidType>();

            VertexID<VidType> vid = null;

            try {
                vid = createVid();
            } catch (Exception e) {
                e.printStackTrace();
            }

            PropertyMap pm = new PropertyMap();

            vertex.configure(vid, pm);
            vertex.readFields(input);

            graphElement = vertex;

        } else {
            try {
                Edge<VidType> edge =  new Edge<VidType>();

                VertexID<VidType> source = createVid();
                VertexID<VidType> target = createVid();

                StringType  label = new StringType();
                PropertyMap pm    = new PropertyMap();

                edge.configure(source, target, label, pm);
                edge.readFields(input);

                graphElement = edge;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Writes the {@code SerializedGraphElement} to an output stream.
     * @param {@code output}  The output stream.
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {

        boolean isVertex = graphElement.isVertex();

        output.writeBoolean(isVertex);

        graphElement.write(output);
    }

    /**
     * Equality check.
     * @param object  Object being compared against the {@code SerializedGraphElement} for equality.
     * @return   {@literal true} if and only if the incoming object is another {@code SerializedGraphElement} and its
     * underlying {@code GraphElement} is equal to that of this {@code SerializedGraphElement} by the {@code equals} test.
     */
    @Override
    public boolean equals (Object object) {
        if (object instanceof SerializedGraphElement) {
            GraphElement incomingGraphElement = ((SerializedGraphElement) object).graphElement();
            if (this.graphElement().isNull()) {
                return (incomingGraphElement.isNull());
            } else {
                return this.graphElement().equals(incomingGraphElement);
            }
        } else {
            return false;
        }
    }

    /**
     * Hash code of the {@code SerializedGraphElement}
     * @return  {@literal 0} if the underlying {@code GraphELement} is {@literal null}, hash code of the underlying
     * {@code GraphElement} otherwise.
     */
    @Override
    public int hashCode() {
        if (this.graphElement() == null) {
            return 0;
        } else {
            return this.graphElement().hashCode();
        }
    }

    /**
     * Compare the {@code SerializedGraphElement} against an {@code Object} using their hashcodes as integers.
     * @param object  The object to be compared against the {@code SerializedGraphElement}
     * @return {@literal -1} if this {@code SerializedGraphElement} has a hashcode strictly less than that of the
     * incoming {@code Object}, 0 if the two hashcodes are equal, and 1 if the hashcode of the {@code SerializedGraphElement}
     * is strictly greater than the of the incoming {@code Object}
     */
    @Override
    public int compareTo(Object object) {

        int thisHash = this.hashCode();

        int thatHash = (object != null) ? object.hashCode() : 0;

        if (thisHash < thatHash) {
            return -1;
        } else if (thisHash == thatHash) {
            return 0;
        } else {
            return 1;
        }
    }
}



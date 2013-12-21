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

import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;
import com.intel.hadoop.graphbuilder.util.HashUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Represents an Edge object with a source, destination, label, and a 
 *(possibly empty) property map.
 * <p>
 * This class is mutable. See the {@code configure} and {@code setProperty} 
 * methods.
 * </p>
 *
 * @param {@code <VidType>} The type of vertex id.
 */
public class Edge<VidType extends WritableComparable<VidType>>  extends GraphElement implements Writable {

    private VertexID<VidType>     src;
    private VertexID<VidType>     dst;
    private StringType  label;
    private PropertyMap properties;

    /**
     * Creates a placeholder edge.
     */

    public Edge() {
        super();

        this.properties = new PropertyMap();
    }

    /**
     * Creates an edge with a given source, destination and label.
     *
     * @param {@code src}   The vertex ID of the edge's source vertex.
     * @param {@code dst}   The vertex ID of the edge's destination vertex.
     * @param {@code label} The edge label.
     */
    public Edge(VertexID<VidType> src, VertexID<VidType> dst, StringType label) {

        this(src, dst, label, new PropertyMap());
    }

    /**
     * Creates an edge with given source name, source label, destination name, destination label, and edge label..
     *
     * @param srcName the name of the source vertex.
     * @param srcLabel the label of the source vertex
     * @param dstName the name of the destination vertex
     * @param dstLabel the label of the destination vertex
     * @param edgeLabel the label of the edge
     */
    public Edge(VidType srcName, StringType srcLabel, VidType dstName, StringType dstLabel, StringType edgeLabel) {
        super();

        VertexID<VidType> srcId = new VertexID<VidType>(srcName, srcLabel);
        VertexID<VidType> dstId = new VertexID<VidType>(dstName, dstLabel);
        this.src = srcId;
        this.dst = dstId;
        this.label = edgeLabel;
        this.properties=  new PropertyMap();
    }

    /**
     * Creates an edge with given source name, source label, destination name, destination label, and edge label..
     *
     * @param srcName the name of the source vertex.
     * @param dstName the name of the destination vertex
     * @param edgeLabel the label of the edge
     */
    public Edge(VidType srcName, VidType dstName, StringType edgeLabel) {
        super();

        VertexID<VidType> srcId = new VertexID<VidType>(srcName, null);
        VertexID<VidType> dstId = new VertexID<VidType>(dstName, null);
        this.src = srcId;
        this.dst = dstId;
        this.label = edgeLabel;
        this.properties=  new PropertyMap();
    }

    /**
     * Creates an edge with given source name, source label, destination name, destination label, and edge label..
     *
     * @param srcName the name of the source vertex.
     * @param dstName the name of the destination vertex
     * @param edgeLabel the label of the edge
     */
    public Edge(VidType srcName, VidType dstName, StringType edgeLabel, PropertyMap propertyMap) {
        super();

        VertexID<VidType> srcId = new VertexID<VidType>(srcName, null);
        VertexID<VidType> dstId = new VertexID<VidType>(dstName, null);
        this.src = srcId;
        this.dst = dstId;
        this.label = edgeLabel;
        this.properties =  propertyMap;
    }

    /**
     * Creates an edge with given source, destination, label and property map
     *
     * @param {@code sr}     The vertex ID of the edge's source vertex
     * @param {@code dst}    The vertex ID of the edge's destination vertex
     * @param {@code label}  The edge label.
     */
    public Edge(VertexID<VidType> src, VertexID<VidType> dst, StringType label, PropertyMap propertyMap) {
        super();

        this.src = src;
        this.dst = dst;
        this.label = label;
        this.properties = propertyMap;
    }

    /**
     * Creates an edge with given source name, source label, destination name, destination label, edge label, and
     * property map
     *
     * @param srcName the name of the source vertex.
     * @param srcLabel the label of the source vertex
     * @param dstName the name of the destination vertex
     * @param dstLabel the label of the destination vertex
     * @param edgeLabel the label of the edge
     * @param propertyMap the property map of the edge
     */
    public Edge(VidType srcName, StringType srcLabel, VidType dstName, StringType dstLabel, StringType edgeLabel,
                PropertyMap propertyMap) {
        super();

        VertexID<VidType> srcId = new VertexID<VidType>(srcName, srcLabel);
        VertexID<VidType> dstId = new VertexID<VidType>(dstName, dstLabel);
        this.src = srcId;
        this.dst = dstId;
        this.label = edgeLabel;
        this.properties =  propertyMap;
    }

    /**
     * Tell the callee that this {@code GraphElement} is an edge.
     * @return  {@code true}
     */
    @Override
    public boolean isEdge() {
        return true;
    }

    /**
     * Tell the callee that this {@code GraphElement} is not a vertex.
     * @return  {@code false}
     */
    @Override
    public boolean isVertex() {
        return false;
    }

    /**
     * Checks to see if this edge is null. If any of the values are nulls the whole 
	 * thing is null. If we try to write an edge with any null values Graph Builder  
	 * will throw an exception. 
     * @return True or false, based upon the null status of the src, dst, and label.
     */
    @Override
    public boolean isNull(){
        if(this.src == null || this.dst == null || this.label == null){
            return true;
        }else{
            return false;
        }

    }

    /**
     *  Overwrites an edge's fields with the given parameters.
     *  @param {@code src}         The vertex ID of the edge's source vertex.
     *  @param {@code dst}         The vertex ID of the edge's destination vertex.
     *  @param {@code properties}  The edge's property map.
     */
    public void configure(VertexID<VidType> src, VertexID<VidType> dst, StringType label, PropertyMap properties) {
        this.src        = src;
        this.dst        = dst;
        this.label      = label;
        this.properties = properties;
    }

    /**
     * Gets a property from the edge's property map.
     * @param {@code key}  The lookup key for the value in the edge's property map.
     */
    public Object getProperty(String key) {
        return properties.getProperty(key);
    }

    /**
     * Sets an edge property.
     *
     * This changes the property map of the edge.
     *
     * @param {@code key} The lookup key for the value in the edge's 
	 * property map.
     * @param {@code val} The value to add to the property map (if the 
	 * key is not there already) and that we overwrite the existing value
	 *(if the key is already in the property map).
     */
    public void setProperty(String key, Writable val) {
        properties.setProperty(key, val);
    }

    /**
     * @return The edge label.
     */
    public StringType getLabel() {
        return label;
    }

    /**
     * @return The vertex ID of the edge's source.
     */
    public VertexID<VidType> getSrc() {
        return src;
    }

    /**
     * @return The vertex ID of the edge's destination.
     */
    public VertexID<VidType> getDst() {
        return dst;
    }

    /**
     * Determines if the edge is a loop - that is, if its source and 
	 * destination are the same vertex.
     * @return True, if the edge's source and destination are equal.
     */
    public boolean isSelfEdge() {
        return Objects.equals(src, dst);
    }

    /**
     * @return The edge's property map.
     */
    public PropertyMap getProperties() {
        return properties;
    }

    /**
     * Sets the entire property map for the edge. Overwrites the existing property map.
     * @param {@code propertyMap}  Incoming property map.
     */
    public void setProperties(PropertyMap propertyMap){
        this.properties = propertyMap;
    }

    /**
     * @return Get the graph element.
     *
     */
    public Edge get(){
        return this;
    }

    /**
     * Checks if the input edge is equal to the current object.
     * This is a deep check which means source, destination
     * vertex ID's and all properties are checked to decide
     * equality
     * @param object an object being tested for equality with the edge
     */
    @Override
    public boolean equals(Object object) {
        if (object instanceof Edge) {

        Edge<VidType> edge = (Edge<VidType>) object;

        return (this.src.equals(edge.getSrc()) && this.dst.equals(edge.getDst()) &&
            this.label.equals(edge.getLabel()) && this.properties.equals(edge.getProperties()));

        } else {
            return false;
        }
    }

    /**
     * Override of hashCode to match the override of equals.
     *
     * A deep calcuation that combines the hash of the  source, destination
     * vertex ID's and all properties.
     */
     @Override
     public int hashCode() {
         return HashUtil.hashQuad(src, dst, label, properties);
     }

    /**
     * Gets the edge's ID - that is,  the triple of its source vertex ID, destination vertex ID, and its label.
     * @return  The triple of the edge's source vertex ID, destination vertex ID, and its label.
     */
    public EdgeID getId() {
        return new EdgeID(this.src, this.dst, this.label);
    }

    /**
     * Converts an edge into a string for printing. The properties are 
	 * tab separated.
     * @return  The string form of the edge.
     */
    @Override
    public final String toString() {
        return src.toString() + "\t" + dst.toString() + "\t"
                + label.toString() + "\t" + properties.toString();
    }

    /**
     * Reads an edge from an input stream.
     * @param {@code input} The input stream.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput input) throws IOException {
        src.readFields(input);
        dst.readFields(input);
        label.readFields(input);
        properties.readFields(input);
    }

    /**
     * Writes an edge to an output stream.
     * @param {@code output} The output stream.
     * @throws IOException
     */
    @Override
    public void write(DataOutput output) throws IOException {
        src.write(output);
        dst.write(output);
        label.write(output);
        properties.write(output);
    }
}

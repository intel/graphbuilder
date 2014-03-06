/*
 * Copyright 2014 YarcData LLC 
 * All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * For more about this software visit:
 *      http://www.01.org/GraphBuilder
 */

package com.intel.pig.udf.eval.mappings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.intel.hadoop.graphbuilder.graphelements.Edge;
import com.intel.hadoop.graphbuilder.graphelements.SerializedGraphElementStringTypeVids;
import com.intel.hadoop.graphbuilder.graphelements.Vertex;
import com.intel.hadoop.graphbuilder.types.PropertyMap;
import com.intel.hadoop.graphbuilder.types.StringType;

/**
 * <p>
 * Represents an edge mapping used in generating property graphs
 * </p>
 * <h3>Mapping Format</h3>
 * <p>
 * Mappings are expressed as Pig maps, the mapping contains at least a
 * {@code source}, {@code target} and {@code label} keys and potentially the
 * optional {@code properties} and {@code bidirectional} keys e.g.
 * </p>
 * 
 * <pre>
 * [ 'source' # 'ssn', 'target' # 'mother', 
 *   'label' # 'mother', 
 *   'inverseLabel' # 'child', 
 *   'properties' # ( 'dob' ),
 *   'inverseProperties' # ( 'dob' ),
 *   'bidirectional' # 'false' ]
 * </pre>
 * 
 * <h4>source</h4>
 * <p>
 * The {@code source} key provides a field name which is used to extract a
 * vertex ID for the source vertex.
 * </p>
 * <h4>target</h4>
 * <p>
 * The {@code target} key provides a field name which is used to extract a
 * vertex ID for the target vertex.
 * </p>
 * <h4>label</h4>
 * <p>
 * The {@code label} key provides a label to the generated edges.
 * </p>
 * <h4>inverseLabel</h4>
 * <p>
 * The {@code inverseLabel} key provides a label to generate an inverse edge, if
 * this key is used then {@code bidirectional} is set to assumed to be false
 * unless otherwise set.
 * </p>
 * <h4>properties</h4>
 * <p>
 * The {@code properties} key provides a tuple consisting of field names which
 * should be used to add properties to the generated edge. Field names are used
 * as the resulting property names with property values taking from the actual
 * input data being mapped.
 * </p>
 * <h4>inverseProperties</h4>
 * <p>
 * The {@code inverseProperties} key provides a tuple consisting of field names
 * which should be used to add properties to the generated inverse edge. Field
 * names are used as the resulting property names with property values taking
 * from the actual input data being mapped.
 * </p>
 * <h4>bidirectional</h4>
 * <p>
 * The {@code bidirectional} key provides a {@code true} or {@code false} value
 * indicating whether the edge is bidrectional or directed. Edges default to
 * being bidirectional if this key is not used.
 * </p>
 */
public class EdgeMapping extends AbstractMapping {

    protected static final String SOURCE_FIELD = "source";
    protected static final String TARGET_FIELD = "target";
    protected static final String LABEL = "label";
    protected static final String INVERSE_LABEL = "inverseLabel";
    protected static final String BIDIRECTIONAL = "bidirectional";
    protected static final String INVERSE_PROPERTIES = "inverseProperties";

    private String sourceField, targetField, label, inverseLabel;
    private List<String> properties = new ArrayList<String>();
    private List<String> inverseProperties = new ArrayList<String>();
    private boolean bidirectional;

    /**
     * Creates a new edge mapping
     * 
     * @param source
     *            Source field
     * @param target
     *            Target field
     * @param label
     *            Label
     * @param inverseLabel
     *            Inverse Label
     * @param properties
     *            Properties
     * @param inverseProperties
     *            Inverse properties
     * @param bidirectional
     *            Whether the edge is bi-directional
     */
    public EdgeMapping(String source, String target, String label, String inverseLabel, Collection<String> properties,
            Collection<String> inverseProperties, boolean bidirectional) {
        if (source == null)
            throw new NullPointerException("Source Field for an edge mapping cannot be null");
        if (target == null)
            throw new NullPointerException("Target Field for an edge mapping cannot be null");
        if (label == null)
            throw new NullPointerException("Label for an edge mapping cannot be null");
        this.sourceField = source;
        this.targetField = target;
        this.label = label;
        this.inverseLabel = inverseLabel;
        if (properties != null)
            this.properties.addAll(properties);
        if (inverseProperties != null)
            this.inverseProperties.addAll(inverseProperties);
        this.bidirectional = bidirectional;
    }

    /**
     * Creates a new edge mapping assuming a directed edge
     * 
     * @param source
     *            Source field
     * @param target
     *            Target field
     * @param label
     *            Label
     * @param inverseLabel
     *            Inverse label
     */
    public EdgeMapping(String source, String target, String label, String inverseLabel) {
        this(source, target, label, null, null, null, false);
    }

    /**
     * Creates a new edge mapping assuming an undirected edge
     * 
     * @param source
     *            Source field
     * @param target
     *            Target field
     * @param label
     *            Label
     */
    public EdgeMapping(String source, String target, String label) {
        this(source, target, label, null, null, null, true);
    }

    /**
     * Creates a new edge mapping directly from an Object
     * <p>
     * This constructor assumes that the passed object comes from the processing
     * of a Pig script and thus will be a Map generated from Pig. See the
     * documentation for {@link EdgeMapping} for details of the map format
     * expected.
     * <p>
     * 
     * @param object
     * @throws ExecException
     */
    @SuppressWarnings("unchecked")
    public EdgeMapping(Object object) throws ExecException {
        if (object == null)
            throw new NullPointerException("Cannot create an edge mapping from a null object");
        if (!(object instanceof Map<?, ?>))
            throw new IllegalArgumentException("Cannot create an edge mapping from a non-map object");

        Map<String, Object> edgeMapping = (Map<String, Object>) object;
        this.sourceField = this.getStringValue(edgeMapping, SOURCE_FIELD, true);
        this.targetField = this.getStringValue(edgeMapping, TARGET_FIELD, true);
        this.label = this.getStringValue(edgeMapping, LABEL, true);
        this.inverseLabel = this.getStringValue(edgeMapping, INVERSE_LABEL, false);

        // When loading bidirectional from map default to true if no inverse
        // label and false if there is an inverse label
        this.bidirectional = this.getBooleanValue(edgeMapping, BIDIRECTIONAL, this.inverseLabel == null);
        List<String> ps = this.getListValue(edgeMapping, PROPERTIES, false);
        if (ps != null)
            this.properties.addAll(ps);
        ps = this.getListValue(edgeMapping, INVERSE_PROPERTIES, false);
        if (ps != null)
            this.inverseProperties.addAll(ps);
    }

    /**
     * Gets the source field
     * 
     * @return Source field
     */
    public String getSourceField() {
        return this.sourceField;
    }

    /**
     * Gets the target field
     * 
     * @return Target field
     */
    public String getTargetField() {
        return this.targetField;
    }

    /**
     * Gets the edge label
     * 
     * @return Edge label
     */
    public String getEdgeLabel() {
        return this.label;
    }

    /**
     * Gets the inverse edge label
     * 
     * @return Inverse edge label
     */
    public String getInverseEdgeLabel() {
        return this.inverseLabel;
    }

    /**
     * Gets the iterator of edge property names
     * 
     * @return Edge property names iterator
     */
    public Iterator<String> getProperties() {
        return this.properties.iterator();
    }

    /**
     * Gets the iterator of inverse edge property names
     * 
     * @return Inverse edge property names iterator
     */
    public Iterator<String> getInverseProperties() {
        return this.inverseProperties.iterator();
    }

    /**
     * Gets whether the edge is bi-directional
     * 
     * @return True if bi-directional, false otherwise
     */
    public boolean isBidirectional() {
        return this.bidirectional;
    }

    /**
     * Applies the mapping to the given input
     * 
     * @param input
     *            Input
     * @param fieldMapping
     *            Field Mapping
     * @param output
     *            Output
     * @throws ExecException
     */
    public void apply(Tuple input, Map<String, Integer> fieldMapping, DataBag output) throws ExecException {
        if (input == null)
            return;

        String srcValue = this.getStringValue(input, fieldMapping.get(this.sourceField));
        if (srcValue == null)
            return;
        String targetValue = this.getStringValue(input, fieldMapping.get(this.targetField));
        if (targetValue == null)
            return;

        // Ensure vertices are created
        Vertex<StringType> srcVertex = new Vertex<StringType>(new StringType(srcValue));
        SerializedGraphElementStringTypeVids srcElement = new SerializedGraphElementStringTypeVids();
        srcElement.init(srcVertex);
        output.add(TupleFactory.getInstance().newTuple(srcElement));
        Vertex<StringType> targetVertex = new Vertex<StringType>(new StringType(targetValue));
        SerializedGraphElementStringTypeVids targetElement = new SerializedGraphElementStringTypeVids();
        targetElement.init(targetVertex);
        output.add(TupleFactory.getInstance().newTuple(targetElement));

        // Generate properties for the edge
        PropertyMap edgeProperties = new PropertyMap();
        Iterator<String> ps = this.getProperties();
        while (ps.hasNext()) {
            String pName = ps.next();
            Integer pIndex = fieldMapping.get(pName);
            if (pIndex == null)
                continue;
            Object pValue = input.get(pIndex);
            if (pValue == null)
                continue;
            edgeProperties.setProperty(pName, this.pigTypesToSerializedJavaTypes(pValue, input.getType(pIndex)));
        }

        // Generate the standard edge
        Edge<StringType> edge = new Edge<StringType>(new StringType(srcValue), new StringType(targetValue), new StringType(
                this.label));
        edge.setProperties(edgeProperties);
        SerializedGraphElementStringTypeVids edgeElement = new SerializedGraphElementStringTypeVids();
        edgeElement.init(edge);
        output.add(TupleFactory.getInstance().newTuple(edgeElement));
        if (this.bidirectional) {
            // Generate the opposing edge
            Edge<StringType> opposingEdge = new Edge<StringType>(new StringType(targetValue), new StringType(srcValue),
                    new StringType(this.label));
            opposingEdge.setProperties(edgeProperties);
            SerializedGraphElementStringTypeVids opposingEdgeElement = new SerializedGraphElementStringTypeVids();
            opposingEdgeElement.init(opposingEdge);
            output.add(TupleFactory.getInstance().newTuple(opposingEdgeElement));
        }

        // Generate properties for the inverse edge
        if (this.inverseLabel == null)
            return;
        PropertyMap inverseEdgeProperties = new PropertyMap();
        ps = this.getInverseProperties();
        while (ps.hasNext()) {
            String pName = ps.next();
            Integer pIndex = fieldMapping.get(pName);
            if (pIndex == null)
                continue;
            Object pValue = input.get(pIndex);
            if (pValue == null)
                continue;
            inverseEdgeProperties.setProperty(pName, this.pigTypesToSerializedJavaTypes(pValue, input.getType(pIndex)));
        }

        // Generate the inverse edge
        Edge<StringType> inverseEdge = new Edge<StringType>(new StringType(targetValue), new StringType(srcValue),
                new StringType(this.inverseLabel));
        inverseEdge.setProperties(inverseEdgeProperties);
        SerializedGraphElementStringTypeVids inverseEdgeElement = new SerializedGraphElementStringTypeVids();
        inverseEdgeElement.init(inverseEdge);
        output.add(TupleFactory.getInstance().newTuple(inverseEdgeElement));
        if (this.bidirectional) {
            // Generate the opposing inverse edge
            Edge<StringType> opposingInverseEdge = new Edge<StringType>(new StringType(srcValue), new StringType(targetValue),
                    new StringType(this.inverseLabel));
            opposingInverseEdge.setProperties(inverseEdgeProperties);
            SerializedGraphElementStringTypeVids opposingInverseEdgeElement = new SerializedGraphElementStringTypeVids();
            opposingInverseEdgeElement.init(opposingInverseEdge);
            output.add(TupleFactory.getInstance().newTuple(opposingInverseEdgeElement));
        }
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> mapping = new HashMap<String, Object>();
        mapping.put(SOURCE_FIELD, this.sourceField);
        mapping.put(TARGET_FIELD, this.targetField);
        mapping.put(LABEL, this.label);
        if (this.inverseLabel != null)
            mapping.put(INVERSE_LABEL, this.inverseLabel);
        mapping.put(BIDIRECTIONAL, Boolean.toString(this.bidirectional).toLowerCase());
        if (this.properties.size() > 0)
            mapping.put(PROPERTIES, TupleFactory.getInstance().newTuple(this.properties));
        if (this.inverseProperties.size() > 0)
            mapping.put(INVERSE_PROPERTIES, TupleFactory.getInstance().newTuple(this.inverseProperties));
        return mapping;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        builder.append(" '");
        builder.append(SOURCE_FIELD);
        builder.append("' # '");
        builder.append(this.sourceField);
        builder.append("', '");
        builder.append(TARGET_FIELD);
        builder.append("' # '");
        builder.append(this.targetField);
        builder.append("', '");
        builder.append(LABEL);
        builder.append("' # '");
        builder.append(this.label);
        if (this.inverseLabel != null) {
            builder.append("', '");
            builder.append(INVERSE_LABEL);
            builder.append("' # '");
            builder.append(this.inverseLabel);
        }
        builder.append("', '");
        builder.append(BIDIRECTIONAL);
        builder.append("' # '");
        builder.append(Boolean.toString(this.bidirectional).toLowerCase());
        builder.append('\'');
        if (this.properties.size() > 0) {
            builder.append(", ");
            builder.append(this.tupleToMapKeyValueString(this.properties, PROPERTIES));
        }
        if (this.inverseProperties.size() > 0) {
            builder.append(", ");
            builder.append(this.tupleToMapKeyValueString(this.inverseProperties, INVERSE_PROPERTIES));
        }
        builder.append(" ]");
        return builder.toString();
    }
}

/*
 * Copyright 2014 YarcData LLC All Rights Reserved.
 */

package com.intel.pig.udf.eval.mappings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;

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
 * [ 'source' # 'ssn', 'target' # 'mother', 'label' # 'mother', 'properties' # ( 'dob' ), 'bidirectional' # 'false' ]
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
 * <h4>properties</h4>
 * <p>
 * The {@code properties} key provides a tuple consisting of field names which
 * should be used to add properties to the generated edge. Field names are used
 * as the resulting property names with property values taking from the actual
 * input data being mapped.
 * </p>
 * <h4>bidirectional</h4>
 * <p>
 * The {@code bidirectional} key provides a {@code true} or {@code false} value
 * indicating whether the edge is bidrectional or directed. Edges default to
 * being bidirectional if this key is not used.
 * </p>
 */
public class EdgeMapping extends AbstractMapping {

    /**
     * Map key for the source field
     */
    protected static final String SOURCE_FIELD = "source";
    /**
     * Map key for the target field
     */
    protected static final String TARGET_FIELD = "target";
    /**
     * Map key for the edge label
     */
    protected static final String LABEL = "label";
    /**
     * Map key for the bidirectional flag
     */
    protected static final String BIDIRECTIONAL = "bidirectional";

    private String sourceField, targetField, label;
    private List<String> properties = new ArrayList<String>();
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
     * @param properties
     *            Properties
     * @param bidirectional
     *            Whether the edge is bi-directional
     */
    public EdgeMapping(String source, String target, String label, Collection<String> properties, boolean bidirectional) {
        if (source == null)
            throw new NullPointerException("Source Field for an edge mapping cannot be null");
        if (target == null)
            throw new NullPointerException("Target Field for an edge mapping cannot be null");
        if (label == null)
            throw new NullPointerException("Label for an edge mapping cannot be null");
        this.sourceField = source;
        this.targetField = target;
        this.label = label;
        if (properties != null)
            this.properties.addAll(properties);
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
     */
    public EdgeMapping(String source, String target, String label) {
        this(source, target, label, null, true);
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
        this.bidirectional = this.getBooleanValue(edgeMapping, BIDIRECTIONAL, true);
        List<String> ps = this.getListValue(edgeMapping, PROPERTIES, false);
        if (ps != null)
            this.properties.addAll(ps);
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
     * Gets the iterator of edge property names
     * 
     * @return Edge property names iterator
     */
    public Iterator<String> getProperties() {
        return this.properties.iterator();
    }

    /**
     * Gets whether the edge is bi-directional
     * 
     * @return True if bi-directional, false otherwise
     */
    public boolean isBidirectional() {
        return this.bidirectional;
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
        builder.append("', '");
        builder.append(BIDIRECTIONAL);
        builder.append("' # '");
        builder.append(Boolean.toString(this.bidirectional).toLowerCase());
        builder.append('\'');
        if (this.properties.size() > 0) {
            builder.append(", '");
            builder.append(PROPERTIES);
            builder.append("' # ( ");
            for (int i = 0; i < this.properties.size(); i++) {
                builder.append('\'');
                builder.append(this.properties.get(i));
                builder.append('\'');
                if (i < this.properties.size() - 1) {
                    builder.append(", ");
                }
            }
            builder.append(" )");
        }
        builder.append(" ]");
        return builder.toString();
    }
}
